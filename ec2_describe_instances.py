from typing import Any
import boto3

dynamodb_table = 'demo-table'
dynamodb_instance_id_column = 'instance_id'
dynamodb_instance_name_column = 'instance_name'


def lambda_handler(event, context):
    # Aggregate all ec2 data using boto3
    instances = aggregate_ec2_data()

    # pass the data to insert into dynamoDB
    insert_dynamodb(instances=instances)
    return {
        'statusCode': 200,
        'body': 'Data inserted into DynamoDB successfully'
    }


def aggregate_ec2_data() -> list[Any]:
    # Initialize a session using DigitalOcean Spaces.
    session = boto3.session.Session()
    ec2 = session.client('ec2')

    # Get the list of all EC2 instances
    instances = ec2.describe_instances()['Reservations']
    instances_return = [instance for reservation in instances for instance in reservation['Instances']]
    return instances_return

    # Get the list of all EC2 instances
    """
    instances = ec2.describe_instances()['Reservations']
    instances = [instance for reservation in instances for instance in reservation['Instances']]

    # Get the configuration aggregator data for each instance
    for instance in instances:
        response = ec2.describe_instance_attribute(InstanceId=instance['InstanceId'], Attribute='instanceType')
        #config_aggregator_data = response['Association']

        print(f"Instance ID: {instance['InstanceId']}")
        print(f"Configuration Aggregator Data: {response}")"""


def insert_dynamodb(instances: list):
    session = boto3.session.Session()
    dynamodb = session.resource('dynamodb')
    # Get the table
    table = dynamodb.Table(dynamodb_table)
    # Get the names of the instances
    for instance in instances:
        instance_id = instance['InstanceId']
        tags = instance.get('Tags', [])
        instance_name = next((tag['Value'] for tag in tags if tag['Key'] == 'Name'), 'N/A')
        # Insert data into the table
        table.put_item(
            Item={
                dynamodb_instance_id_column: instance_id,
                dynamodb_instance_name_column: instance_name
            }
        )
