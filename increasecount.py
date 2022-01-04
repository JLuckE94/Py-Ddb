from decimal import Decimal
from pprint import pprint
import boto3
from datetime import date

def increase_rating(viewcount, lastupdated, dynamodb=None):
    if not dynamodb:
        dynamodb = boto3.resource('dynamodb', endpoint_url="http://localhost:8000")

    table = dynamodb.Table('ViewCountAPI')

    response = table.update_item(
        Key={
            'viewcount': viewcount,
            'lastupdated': lastupdated
        },
        ReturnValues="UPDATED_NEW"
    )
    return response


if __name__ == '__main__':
    update_response = increase_rating(0, date.today)
