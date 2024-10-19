import json
import boto3

dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('UserSearchHistory')

def lambda_handler(event, context):
    user_id = event['userId']  # Assume userId is passed in the event
    last_location = event['lastLocation']
    last_category = event['lastCategory']

    # Save the user's last search to DynamoDB
    table.put_item(
        Item={
            'UserID': user_id,
            'LastLocation': last_location,
            'LastCategory': last_category
        }
    )

    return {
        'statusCode': 200,
        'body': json.dumps('User search saved successfully!')
    }
