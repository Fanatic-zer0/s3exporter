# S3 Exporter

A simple and lightweight Python exporter that collects AWS S3 bucket metrics and exposes them in Prometheus format for easy monitoring and alerting.

## Features
- Tracks total object count, size, and largest object per bucket/prefix.  
- Monitors S3 list operation latency and success rate.  
- Detects and counts newly uploaded objects.  
- Supports multiple buckets and prefixes using regex patterns.  
- Runs as a standalone HTTP exporter for Prometheus to scrape.

## Metrics
| Metric Name | Description |
|--------------|-------------|
| `s3_bucket_objects_total` | Total number of objects in the bucket/prefix. |
| `s3_bucket_objects_size_bytes_total` | Total size of all objects in bytes. |
| `s3_bucket_max_object_size_bytes` | Size of the largest object. |
| `s3_bucket_last_modified_seconds` | UNIX timestamp of the last modified object. |
| `s3_bucket_last_modified_object_size_bytes` | Size of the most recently modified object. |
| `s3_list_objects_duration_seconds` | Duration of the ListObjectsV2 operation. |
| `s3_list_objects_success` | Success status of ListObjectsV2 operation (1 or 0). |
| `s3_objects_uploaded_total` | Count of newly uploaded objects since last check. |

## Configuration
The exporter loads configuration from Kubernetes configmaps or environment variables:
- `bucket_configs` – List of bucket/prefix regex patterns to monitor.
- `aws_region` – AWS region (default: `us-east-1`).
- `check_interval` – Interval (in seconds) between S3 scans.
- `port` – Prometheus metrics server port (default: `9090`).

## Prerequisites

1. **IRSA Role** (IAM Roles for Service Accounts):

```bash
eksctl create iamserviceaccount
--name s3-exporter
--namespace monitoring
--cluster your-cluster
--attach-policy-arn arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess
--approve
```

2. **Prometheus Operator** with ServiceMonitor support

## Values

| Key | Description | Default |
|-----|-------------|---------|
| `config.bucketConfigs` | JSON array of bucket/prefix patterns | `[]` |
| `config.checkInterval` | S3 scan interval (seconds) | `300` |
| `serviceMonitor.enabled` | Enable ServiceMonitor | `true` |


## Full Installation

helm repo add s3-exporter https://Fanatic-zer0.github.io/s3-exporter-helm/
helm repo update
helm install s3-exporter s3-exporter/s3-exporter
--namespace monitoring
--set config.bucketConfigs='[{"bucket":"^prod-.*","prefix":"logs/"}]'
--set image.repository=your-registry/s3-exporter:latest

