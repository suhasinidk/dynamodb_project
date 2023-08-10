import boto3
import random
def lambda_handler(event, context): 
    pk = str(random.randint(1, 100))
    item_data = {
    'Employee_ID': {'S': pk},  # 'S' for String, use 'N' for Number if the attribute is a number
    'Employee_Dept': {'S': 'developer'+pk},
    'Lead': {'S': 'John Doe'+pk},
    'Skills': {'S': 'sql'}}
    create_dynamodb_item("table1",item_data)
    dbscaneditem= scan_dynamodb_table("table1")
    print ("Scaned items from Table 1", dbscaneditem)
    quereditems= query_dynamodb_table("table1" , 'Employee_ID', pk)
    print("quereditems",quereditems)
    delete_dynamodb_item("table1", 'Employee_ID', pk)


def delete_dynamodb_item(table_name, primary_key_name, primary_key_value):
    dynamodb = boto3.client('dynamodb')

    try:
        response = dynamodb.delete_item(
            TableName=table_name,
            Key={
                primary_key_name: {
                    'S': primary_key_value  # 'S' for String, use 'N' for Number if the attribute is a number
                }
            }
        )
        print(f"Item with primary key '{primary_key_name}={primary_key_value}' deleted successfully: {response}")
        return response
    except Exception as e:
        print(f"Error deleting item with primary key '{primary_key_name}={primary_key_value}': {e}")
        raise e


def query_dynamodb_table(table_name, attribute_name, attribute_value):

    dynamodb = boto3.client('dynamodb')

    try:
        response = dynamodb.query(
            TableName=table_name,
            KeyConditionExpression=f'#attr = :val',
            ExpressionAttributeNames={
                '#attr': attribute_name
            },
            ExpressionAttributeValues={
                ':val': {
                    'S': attribute_value  # 'S' for String, use 'N' for Number if the attribute is a number
                }
            }
        )
        items = response['Items']
        while 'LastEvaluatedKey' in response:
            response = dynamodb.query(
                TableName=table_name,
                KeyConditionExpression=f'#attr = :val',
                ExpressionAttributeNames={
                    '#attr': attribute_name
                },
                ExpressionAttributeValues={
                    ':val': {
                        'S': attribute_value
                    }
                },
                ExclusiveStartKey=response['LastEvaluatedKey']
            )
            items.extend(response['Items'])
        return items
    except Exception as e:
        print("Error querying table:", e)
        raise e

def scan_dynamodb_table(table_name):
    # Create a Boto3 DynamoDB client
    dynamodb = boto3.client('dynamodb')

    try:
        response = dynamodb.scan(TableName=table_name)
        items = response['Items']
        while 'LastEvaluatedKey' in response:
            response = dynamodb.scan(TableName=table_name, ExclusiveStartKey=response['LastEvaluatedKey'])
            items.extend(response['Items'])
        return items
    except Exception as e:
        print("Error scanning table:", e)
        raise e


def create_dynamodb_item(table_name, item_data):
    # Replace 'YOUR_REGION' with your AWS region
    region_name = 'us-east-1'

    # Create a Boto3 DynamoDB client
    dynamodb = boto3.client('dynamodb', region_name=region_name)

    try:
        response = dynamodb.put_item(
            TableName=table_name,
            Item=item_data
        )
        print("Item created successfully:", response)
        return response
    except Exception as e:
        print("Error creating item:", e)
        raise e
