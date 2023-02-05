import boto3
import json

# Build up Expression Query
AGGREGATOR_QUERY = {
    'Expression': '''
    SELECT accountId, resourceId, awsRegion, configuration,
    configurationItemCaptureTime, tags
    WHERE
    resourceType = 'AWS::EC2::Instance'
    '''
}

# Declare global variables
dynamodb_table = 'demo-table'
dynamodb_instance_id_column = 'instance_id'
dynamodb_instance_name_column = 'instance_name'


def lambda_handler(event, context):
    # collect config aggregator data
    results = collect_config_aggregator()
    # pass the data from config to insert into dynamodb
    insert_dynamodb(instances=results)
    return {
        'statusCode': 200,
        'body': 'Data inserted into DynamoDB successfully'
    }


def collect_config_aggregator() -> list:
    # create boto3 session
    session = boto3.session.Session()
    # create low level config client
    config = session.client('config')
    # collect the data
    response = config.select_resource_config(**AGGREGATOR_QUERY)
    # return the response
    return response['Results']


def insert_dynamodb(instances: list):
    # create boto3 session
    session = boto3.session.Session()
    # create dynamodb low level client
    dynamodb = session.resource('dynamodb')
    # Get the table
    table = dynamodb.Table(dynamodb_table)
    # Get the names of the instances
    for instance in instances:
        instance = json.loads(instance)
        instance_id = instance['resourceId']
        tags = instance['tags']
        instance_name = next((tag['value'] for tag in tags if tag['key'] == 'Name'), 'N/A')
        # Insert data into the table
        table.put_item(
            Item={
                dynamodb_instance_id_column: instance_id,
                dynamodb_instance_name_column: instance_name
            }
        )
