import json
import time
import boto3
import botocore

from utils import get_config, get_cluster_status


def create_iam(config):
    """
    Create IAM role and apply policy to read from S3 bucket
    """
    IAM = boto3.client('iam', aws_access_key_id=config.get('AWS_ACCESS', 'AWS_ACCESS_KEY_ID'),
                       aws_secret_access_key=config.get('AWS_ACCESS', 'AWS_SECRET_ACCESS_KEY'),
                       region_name=config.get('AWS_ACCESS', 'AWS_REGION'))
    
    iam_role_name = config.get('IAM_ROLE', 'NAME')

    try:
        role = IAM.create_role(
            RoleName=iam_role_name,
            Description='Allows redshift to other AWS services',
            AssumeRolePolicyDocument=json.dumps({
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Effect": "Allow",
                        "Principal": {
                            "Service": "redshift.amazonaws.com"
                        },
                        "Action": "sts:AssumeRole"
                    }
                ]
            })
        )
        print('IAM Role Arn:', role['Role']['Arn'])

    except IAM.exceptions.EntityAlreadyExistsException as e:
        print("IAM already exists: {}".format(e))

    try:
        IAM.attach_role_policy(
            RoleName=iam_role_name,
            PolicyArn=config.get('IAM_ROLE', 'ARN')
        )
    except Exception as e:
        print(e)
    
    print('Sucessfully created the role and attached S3 read only policy...')

    return role

    
def create_redshift_cluster(config, iam_role):
    """
    Initiate a redshift cluster
    """
    redshift = boto3.client(
        "redhsift",
        aws_access_key_id=config.get('AWS_ACCESS', 'AWS_ACCESS_KEY_ID'),
        aws_secret_access_key=config.get('AWS_ACCESS', 'AWS_SECRET_ACCESS_KEY'),
        region_name = config.get('AWS_ACCESS', 'AWS_REGION'),
    )

    print ("Creating redshift cluster...")

    try:
        response = redshift.create_cluster(
            DBName=config.get('CLUSTER', 'DB_NAME'),
            ClusterIdentifier=config.get('CLUSTER', 'CLUSTER_IDENTIFIER'),
            MasterUsername=config.get('CLUSTER', 'DB_USER'),
            MasterUserPassword=config.get('CLUSTER', 'DB_PASSWORD'),
            NodeType=config.get('CLUSTER', 'NODE_TYPE'),
            Port = int(config.get('CLUSTER', 'DB_PORT')),
            IamRoles = [
                iam_role['Role']['Arn']
            ],

            NumberOfNodes=int(config.get('CLUSTER', 'NODE_COUNT'))
        )

        print("Create cluster to AWS call complete...")

    except Exception as e:
        print ('Cluster not created : {}'.format(e))

    cluster_start_time = time.time()
    status_checked = False

    while True:
        print("Getting cluster status...")
        cluster_status = get_cluster_status(config, redshift)
        status_checked = True

        if cluster_status['ClusterStatus'] == 'available':
            break

        print('Cluster Status : {}'.format(cluster_status))
        print('Status Checked : {}'.format(status_checked))
        print("Total time cluster started : {}".format(time.time() - cluster_start_time))

def main():

    config = get_config()
    iam_role = create_iam(config)
    create_redshift_cluster(config, iam_role)

if __name__ == "__main__":
    main()





