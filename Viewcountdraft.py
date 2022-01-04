
from pprint import pprint
from datetime import date
import boto3
from decimal import Decimal
from botocore.exceptions import ClientError
#1 make sure python+dynamo client work
#2 Create table frame
#3 put in object
#4 get object
#5 update object
#Mess around with last updated remove from main key and replace with secondary ID and increment lastudpated, it will basically change days.
table_name = 'ViewCountv1'
#variable way get_dynamodb =  dynamodb = boto3.resource('dynamodb', endpoint_url="http://localhost:8000")
#EX: Reusable function 
# def get_dynamodb():
    #dynamodb = boto3.resource('dynamodb', endpoint_url="http://localhost:8000")
    #return dynamodb


def create_table(dynamodb=None):#create table
    if not dynamodb:
        dynamodb = boto3.resource('dynamodb', endpoint_url="http://localhost:8000")

    table = dynamodb.create_table(
        TableName= table_name,
        KeySchema=[
            {
                'AttributeName': 'Id',
                'KeyType': 'HASH'  # Partition key
            },
            {
                'AttributeName': 'LastUpdated',
                'KeyType': 'RANGE'  # Sort key
            },
        ],
        AttributeDefinitions=[
            {
                'AttributeName': 'Id',
                'AttributeType': 'N'
            },
            {
                'AttributeName': 'LastUpdated',
                'AttributeType': 'S'
            },
        ],
        ProvisionedThroughput={
            'ReadCapacityUnits': 10,
            'WriteCapacityUnits': 10
        }
    )
    return table

def put_item(viewcount, lastupdated, Id, dynamodb=None):#item
    if not dynamodb:
        dynamodb = boto3.resource('dynamodb', endpoint_url="http://localhost:8000")

    table = dynamodb.Table(table_name)
    response = table.put_item(
       Item={
            'ViewCount': viewcount,
            'LastUpdated': lastupdated,
            'Id': Id
            }
        )

    return response


def increment_counter(Id, dynamodb=None):#udpate item
    if not dynamodb:
        dynamodb = boto3.resource('dynamodb', endpoint_url="http://localhost:8000")

    table = dynamodb.Table(table_name)

    response = table.update_item(
        Key={
            'Id': Id,
            'LastUpdated': '20211221'
        },
        UpdateExpression='set ViewCount = ViewCount + :val',
        ExpressionAttributeValues={':val': Decimal(1)},
        ReturnValues="UPDATED_NEW"
    )
    return response


def get_count(Id, dynamodb=None):#get item
    if not dynamodb:
        dynamodb = boto3.resource('dynamodb', endpoint_url="http://localhost:8000")

    table = dynamodb.Table(table_name)

    try:
        response = table.get_item(Key={'Id': Id, 'LastUpdated': '20211221'})
    except ClientError as e:
        print(e.response['Error']['Message'])
    else:
        return response['Item']


if __name__ == '__main__':
    #put_response = put_item(0, '20211221', 1)
    #pprint(put_response)
    pprint(get_count(1))
    #create_table_response = create_table()
    #pprint(create_table_response)

    #for i in range(5):
        #pprint(increment_counter(1))
    
