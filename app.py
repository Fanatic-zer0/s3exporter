import boto3
import time
import re
import logging
import schedule
from collections import defaultdict, deque
from prometheus_client import start_http_server, Gauge, Counter
from botocore.client import Config as BotoConfig
from config import load_settings_from_kubernetes, parse_bucket_configs

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger("s3_exporter")

# ----------------------- INITIALIZE PROMETHEUS METRICS --------------------------------
def init_metrics(bucket_configs, max_cache_size=100000):
    """Initialize and return all Prometheus metrics dictionaries + simple cache management"""

    metrics = {
        "s3_bucket_max_object_size_bytes": Gauge(
            "s3_bucket_max_object_size_bytes",
            "Size in bytes of the largest object in the S3 bucket/prefix",
            ["bucket", "prefix"]
        ),
        "s3_bucket_objects_total": Gauge(
            "s3_bucket_objects_total",
            "Total number of objects in the S3 bucket/prefix",
            ["bucket", "prefix"]
        ),
        "s3_bucket_objects_size_bytes_total": Gauge(
            "s3_bucket_objects_size_bytes_total",
            "Total size in bytes of all objects in the S3 bucket/prefix",
            ["bucket", "prefix"]
        ),
        "s3_bucket_last_modified_seconds": Gauge(
            "s3_bucket_last_modified_seconds",
            "UNIX timestamp of the most recently modified object in the S3 bucket/prefix",
            ["bucket", "prefix"]
        ),
        "s3_bucket_last_modified_object_size_bytes": Gauge(
            "s3_bucket_last_modified_object_size_bytes",
            "Size in bytes of the most recently modified object in the S3 bucket/prefix",
            ["bucket", "prefix"]
        ),
        "s3_list_objects_duration_seconds": Gauge(
            "s3_list_objects_duration_seconds",
            "Duration in seconds of S3 ListObjects operations",
            ["bucket", "prefix", "operation"]
        ),
        "s3_list_objects_success": Gauge(
            "s3_list_objects_success",
            "S3 ListObjects operation success status (1=success, 0=error)",
            ["bucket", "prefix", "operation"]
        ),
        "s3_objects_uploaded_total": Counter(
            "s3_objects_uploaded_total",
            "Cumulative count of new objects uploaded to S3 bucket/prefix since exporter start",
            ["bucket", "prefix"]
        ),
        "_objects_seen": defaultdict(lambda: deque(maxlen=max_cache_size))
    }

    # Initialize metrics with zero values for all configured buckets/prefixes
    for cfg in bucket_configs:
        bucket = cfg.get("bucket", "unknown")
        prefix = cfg.get("prefix", "")
        metrics["s3_bucket_max_object_size_bytes"].labels(bucket, prefix).set(0)
        metrics["s3_bucket_objects_total"].labels(bucket, prefix).set(0)
        metrics["s3_bucket_objects_size_bytes_total"].labels(bucket, prefix).set(0)
        metrics["s3_bucket_last_modified_seconds"].labels(bucket, prefix).set(0)
        metrics["s3_bucket_last_modified_object_size_bytes"].labels(bucket, prefix).set(0)
        metrics["s3_list_objects_success"].labels(bucket, prefix, "list_objects_v2").set(0)
        metrics["s3_list_objects_duration_seconds"].labels(bucket, prefix, "list_objects_v2").set(0)

    return metrics

# ------------------------ HANDLING REGEX PATTERN------------------------
def compile_bucket_patterns(bucket_cfgs):
    processed = []
    for cfg in bucket_cfgs:
        try:
            pattern = cfg["bucket"]
            if not pattern.startswith("^"):
                pattern = "^" + pattern
            regex = re.compile(pattern)
            processed.append({"bucket_regex": regex, "prefix": cfg["prefix"]})
            logger.info(f"Compiled bucket regex: {pattern}")
        except Exception as e:
            logger.error(f"Invalid bucket config {cfg}: {e}")
    return processed

# ------------------------ INITIALIZE S3 CLIENT  ------------------------
def make_s3_client(settings, region=None):
    """Return an S3 boto3 client, default or regional."""
    try:
        return boto3.client(
            "s3",
            region_name=region or settings.get("aws_region", "us-east-1"),
            config=BotoConfig(
                signature_version="s3v4",
                s3={"addressing_style": "virtual"},
                retries={'max_attempts': 3, 'mode': 'standard'}
            )
        )
    except Exception as e:
        logger.error(f"Failed to create S3 client for region {region}: {e}")
        raise

# ------------------------ OBJECT PROCESSING  ------------------------
def process_objects(bucket, prefix, contents, metrics):
    """Process a list of objects and return aggregated stats"""
    stats = {
        "count": 0,
        "size_sum": 0,
        "biggest_size": 0,
        "last_modified_time": 0,
        "last_modified_size": 0,
    }

    cache_key = f"{bucket}:{prefix}"
    obj_keys = [obj["Key"] for obj in contents if not obj["Key"].endswith("/")]
    seen_deque = metrics["_objects_seen"][cache_key]
    seen_set = set(seen_deque)
    new_objs = [k for k in obj_keys if k not in seen_set]

    if new_objs:
        metrics["s3_objects_uploaded_total"].labels(bucket, prefix).inc(len(new_objs))
        # Add new objects to cache (old ones automatically depending upon MAx length)
        for k in new_objs:
            seen_deque.append(k)
        logger.info(f"Found {len(new_objs)} new objects in {bucket}/{prefix}")

    for obj in contents:
        if obj["Key"].endswith("/"):
            continue

        size = obj["Size"]
        mtime = obj["LastModified"].timestamp()

        stats["count"] += 1
        stats["size_sum"] += size
        stats["biggest_size"] = max(stats["biggest_size"], size)

        if mtime > stats["last_modified_time"]:
            stats["last_modified_time"] = mtime
            stats["last_modified_size"] = size

    return stats

def update_metrics(bucket, prefix, stats, metrics):
    """Update Prometheus metrics with aggregated stats."""
    metrics["s3_bucket_max_object_size_bytes"].labels(bucket, prefix).set(stats["biggest_size"])
    metrics["s3_bucket_objects_total"].labels(bucket, prefix).set(stats["count"])
    metrics["s3_bucket_objects_size_bytes_total"].labels(bucket, prefix).set(stats["size_sum"])

    if stats["last_modified_time"] > 0:
        metrics["s3_bucket_last_modified_seconds"].labels(bucket, prefix).set(stats["last_modified_time"])
        metrics["s3_bucket_last_modified_object_size_bytes"].labels(bucket, prefix).set(stats["last_modified_size"])

# -------------------------- BUCKET CHECK ---------------------------------------
def check_bucket(bucket, prefix, client, metrics):
    start = time.time()
    try:
        paginator = client.get_paginator("list_objects_v2")
        all_objects = []
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            all_objects.extend(page.get("Contents", []))

        stats = process_objects(bucket, prefix, all_objects, metrics)
        update_metrics(bucket, prefix, stats, metrics)

        elapsed = time.time() - start
        metrics["s3_list_objects_success"].labels(bucket, prefix, "list_objects_v2").set(1)
        metrics["s3_list_objects_duration_seconds"].labels(bucket, prefix, "list_objects_v2").set(elapsed)
        # Log cache size for monitoring
        cache_key = f"{bucket}:{prefix}"
        cache_size = len(metrics["_objects_seen"][cache_key])
        logger.info(
            f"Checked {bucket}/{prefix} -> {stats['count']} objs "
            f"({stats['size_sum']/1024/1024:.2f} MB) in {elapsed:.2f}s, "
            f"cache size: {cache_size}"
        )
    except Exception as e:
        logger.error(f"Error checking {bucket}/{prefix}: {e}")
        metrics["s3_list_objects_success"].labels(bucket, prefix, "list_objects_v2").set(0)

# -------------------------- MONITORING LOOP ------------------------------------
def monitor(bucket_cfgs, metrics, base_client, settings):
    logger.info("Starting a monitoring cycle...")
    try:
        all_buckets = [b["Name"] for b in base_client.list_buckets()["Buckets"]]

        for cfg in bucket_cfgs:
            regex, prefix = cfg["bucket_regex"], cfg["prefix"]
            matched = [b for b in all_buckets if regex.match(b)]
            if not matched:
                logger.warning(f"No buckets match {regex.pattern}")
                continue
            for bucket in matched:
                try:
                    region_info = base_client.get_bucket_location(Bucket=bucket)
                    region = region_info.get("LocationConstraint") or "us-east-1"

                    regional_client = make_s3_client(settings, region)
                    check_bucket(bucket, prefix, regional_client, metrics)
                except Exception as e:
                    logger.error(f"Error checking bucket {bucket}: {e}")

    except Exception as e:
        logger.error(f"Error in monitor cycle: {e}")
    finally:
        # Log total cache statistics
        total_cached_objects = sum(len(cache) for cache in metrics["_objects_seen"].values())
        logger.info(f"Monitoring cycle complete. Total cached objects: {total_cached_objects}")
        
# ------------------------ MAIN ENTRYPOINT ------------------------------------
def main():
    try:
        settings = load_settings_from_kubernetes()
        bucket_cfgs = parse_bucket_configs(settings["bucket_configs"])
        compiled_cfgs = compile_bucket_patterns(bucket_cfgs)

        # Initialize metrics with configurable cache size
        cache_size = settings.get("cache_size", 10000)  # Default 10000 objects per bucket/prefix
        metrics = init_metrics(bucket_cfgs, max_cache_size=cache_size)
        
        base_client = make_s3_client(settings)

        start_http_server(settings["port"])
        logger.info(f"Prometheus server started on :{settings['port']}")
        logger.info(f"Cache size per bucket/prefix: {cache_size}")
        schedule.every(settings["check_interval"]).seconds.do(
            monitor, compiled_cfgs, metrics, base_client, settings
        )

        #### first run after app starts.
        monitor(compiled_cfgs, metrics, base_client, settings)

        while True:
            schedule.run_pending()
            time.sleep(1)
    except Exception as e:
        logger.exception(f"Fatal error in main: {e}")
        raise

if __name__ == "__main__":
    main()
