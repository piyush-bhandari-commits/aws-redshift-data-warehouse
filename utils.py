import configparser
import psycopg2 as pg

def get_config():
    """
    Loads the configuration object from the config file
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    return config


def connect_to_database():
    """
    Connects to redhift database
    """
    config = get_config()

    HOST = config.get('CLUSTER', 'HOST')
    DB_NAME = config.get('CLUSTER', 'DB_NAME')
    DB_USER = config.get('CLUSTER', 'DB_USER')
    DB_PASSWORD = config.get('CLUSTER', 'DB_PASSWORD')
    DB_PORT = config.get('CLUSTER', 'DB_PORT')
    CONNECTION_STRING = "host={} dbname={} user={} password={} port={}".format(HOST, DB_NAME, DB_USER, DB_PASSWORD, DB_PORT)

    print("Connecting to database...", CONNECTION_STRING)
    conn = pg.connect(CONNECTION_STRING)
    print("Connection to database is successful...")

    return conn


def get_cluster_status(config, boto3_redshift):
    """
    Get the cluster status for redshift
    config: configuration object
    boto3_redshift: redhsift client

    status: 
    True= cluster is running , 
    False= cluster is not running  and 
    None= cluster not created
    """
    try:
        status = boto3_redshift.describe_clusters(
            ClusterIdentifier = config.get('CLUSTER', 'CLUSTER_IDENTIFIER'))
    except Exception as e:
        print("Cluster status not found!!!", e)
        return None
    
    return status['Clusters'][0]
    
    
