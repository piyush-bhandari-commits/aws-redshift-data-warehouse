import json
import time
import boto3
import botocore

from utils import get_config, get_cluster_status


def create_iam():
    """
    Create IAM role and apply policy to read from S3 bucket
    """
    IAM = boto3.client('')
    pass