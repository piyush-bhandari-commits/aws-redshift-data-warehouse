import time
import configparser
import boto3
import botocore

from utils import get_config, get_cluster_status


def remove_iam(config):

    IAM = boto3.client(
        'iam',
        aws_access_key_id = config.get('AWS_ACCESS', 'AWS_ACCESS_KEY_ID'),
        aws_secret_access_key = config.get('AWS_ACCESS', 'AWS_SECRET_ACCESS_KEY'),
        region_name = config.get('AWS_ACCESS', 'AWS_REGION')
    )

    iam_role_name = config.get('IAM_ROLE', 'NAME')
    iam_policy_arn = config.get('IAM_ROLE', 'ARN')

    try:
        IAM.detach_role_policy(
            RoleName=iam_role_name, 
            PolicyArn=config.get('IAM_ROLE', 'ARN')
        )

        print("Role policy removed")

    except Exception as e:

        print("Could not remove role policy {}".format(e))


    try:
        IAM.delete_role(
            RoleName = iam_role_name
        )

        print ("Role removed...")

    except Exception as e:
        print ('Could not remove role {}'.format(e))

def delete_redshift_cluster(config):

    redshift = boto3.client(
        'redshift',
        aws_access_key_id = config.get('AWS_ACCESS', 'AWS_ACCESS_KEY_ID'),
        aws_secret_access_key = config.get('AWS_ACCESS', 'AWS_SECRET_ACCESS_KEY'),
        region_name = config.get('AWS_ACCESS', 'AWS_REGION'),
    )

    try:
        redshift.delete_cluster(
            ClusterIdentifier=config.get('CLUSTER', 'CLUSTER_IDENTIFIER'),
            SkipFinalClusterSnapshot=True
        )

        print('Delete redshift cluster...')

    except Exception as e:
        print("Could not delete redshift cluster : {}".format(e))

    cluster_delete_time = time.time()

    while True:

        cluster_status = get_cluster_status(config, redshift)

        if cluster_status is None:
            print('Cluster is deleted...')
            break

        print('Cluster is : {}'.format(cluster_status['ClusterStatus']))

        time.sleep(5)

        print('Time delete intiated {}'.format(time.time() - cluster_delete_time))

def main():

    config = get_config()
    remove_iam(config)
    delete_redshift_cluster(config)


if __name__ == "__main__":
    main()