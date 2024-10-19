import json
import boto3

dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('UserSearchHistory')

def lambda_handler(event, context):
    user_id = event['userId'] 

    # Fetch the user's last search from DynamoDB
    response = table.get_item(Key={'UserID': user_id})

    if 'Item' in response:
        last_location = response['Item'].get('LastLocation')
        last_category = response['Item'].get('LastCategory')
        return {
            'statusCode': 200,
            'body': json.dumps({
                'LastLocation': last_location,
                'LastCategory': last_category,
                'Recommendation': f"Based on your last search in {last_location} for {last_category}, here are some recommendations..."
            })
        }
    else:
        return {
            'statusCode': 404,
            'body': json.dumps('No previous search found.')
        }
