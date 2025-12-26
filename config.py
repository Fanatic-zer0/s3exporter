import json
import os
import logging

logger = logging.getLogger(__name__)

def load_settings_from_kubernetes(): # mounting from /app/config and secret from pod.
    config = {
        # "aws_access_key_id": None,
        # "aws_secret_access_key": None,
        "aws_region": "us-east-1",
        "check_interval": 300,
        "port": 9340,
        "bucket_configs": "[]"
    }
    
    ### migrating to iam role, not needed anymore

    # secrets_dir = "/app/secrets" #Mounted secret path
    # if os.path.exists(secrets_dir):
    #     try:
    #         with open(os.path.join(secrets_dir, "aws-access-key-id"), "r") as f:
    #             config["aws_access_key_id"] = f.read().strip()
    #         with open(os.path.join(secrets_dir, "aws-secret-access-key"), "r") as f:
    #             config["aws_secret_access_key"] = f.read().strip()
    #     except Exception as e:
    #         logger.error(f"Error loading secrets: {str(e)}")
    #         raise ValueError("Failed to load required AWS credentials") from e

    config_dir = "/app/config" # Mounted config path
    if os.path.exists(config_dir):
        try:
            if os.path.exists(os.path.join(config_dir, "aws-region")):
                with open(os.path.join(config_dir, "aws-region"), "r") as f:
                    config["aws_region"] = f.read().strip()
            if os.path.exists(os.path.join(config_dir, "check-interval")):
                with open(os.path.join(config_dir, "check-interval"), "r") as f:
                    config["check_interval"] = int(f.read().strip())
            if os.path.exists(os.path.join(config_dir, "port")):
                with open(os.path.join(config_dir, "port"), "r") as f:
                    config["port"] = int(f.read().strip())
            if os.path.exists(os.path.join(config_dir, "bucket-configs")):
                with open(os.path.join(config_dir, "bucket-configs"), "r") as f:
                    config["bucket_configs"] = f.read().strip()
        except Exception as e:
            logger.warning(f"Error loading optional configs: {str(e)}")

    return config


def parse_bucket_configs(bucket_configs_raw): #Json Parsing 
    try:
        return json.loads(bucket_configs_raw)
    except json.JSONDecodeError:
        if os.path.exists(bucket_configs_raw):
            with open(bucket_configs_raw, "r") as f:
                return json.load(f)
        return []