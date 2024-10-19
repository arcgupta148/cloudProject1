import json
import boto3
import random
from datetime import datetime
from botocore.exceptions import ClientError

# Hardcoded values for the SQS queue, DynamoDB table, and OpenSearch endpoint
QUEUE_URL = 'https://sqs.us-east-1.amazonaws.com/739275456688/SQS_Q1'  # SQS Queue URL
DYNAMO_TABLE_NAME = 'yelp-restaurants'  # DynamoDB Table Name
ES_ENDPOINT = 'https://search-restaurant-domain-56t3dengujvnovrqxnkjtvreke.aos.us-east-1.on.aws'  # OpenSearch Endpoint
INDEX_NAME = 'restaurants'  # OpenSearch index name

# Initialize AWS clients
sqs = boto3.client('sqs')
dynamodb = boto3.resource('dynamodb')
ses = boto3.client('ses')
es = boto3.client('opensearch')

def lambda_handler(event, context):
    try:
        # Step 1: Pull a message from the SQS queue
        sqs_response = sqs.receive_message(
            QueueUrl=QUEUE_URL,
            MaxNumberOfMessages=1,
            WaitTimeSeconds=10  # Wait time to reduce empty responses
        )
        
        # Check if any messages are available
        if 'Messages' not in sqs_response:
            print("No messages in the queue.")
            return {
                'statusCode': 200,
                'body': json.dumps('No messages in queue.')
            }
        print(f"LF2 is invoked by simple email service")
        # Process the message
        message = sqs_response['Messages'][0]
        message_body = json.loads(message['Body'])
        print(f"Message received from SQS: {message_body}")

        # Extract necessary data from the message body
        cuisine = message_body.get('Cuisine').capitalize()
        location = message_body.get('Location', 'Unknown')
        dining_date = message_body.get('DiningDate', str(datetime.now().date()))  # Default to today
        dining_time = message_body.get('DiningTime', '7:00 PM')  # Default to 7 PM
        number_of_people = message_body.get('NumberOfPeople', '2')
        recipient_email = message_body.get('Email')
        
        print(f"Cuisine: {cuisine}, Location: {location}, Dining Time: {dining_time}, Recipient Email: {recipient_email}")
        
        # Step 2: Get 5 random restaurant recommendations from DynamoDB
        restaurants = get_random_restaurants(cuisine, 5)  # Fetch 5 restaurants
        if not restaurants:
            print("No matching restaurants found.")
            return {
                'statusCode': 500,
                'body': json.dumps('No matching restaurants found.')
            }
        
        # Step 3: Format email body
        email_subject = f"Your Restaurant Recommendations for {cuisine} Cuisine"
        email_body = f"Hello! Here are my {cuisine} restaurant suggestions for {number_of_people} people, for {dining_date} at {dining_time}:\n\n"
        
        # Add each restaurant to the email body
        for idx, restaurant in enumerate(restaurants, 1):
            email_body += f"{idx}. {restaurant['Name']}, located at {restaurant['Address']}\n"
        
        # Step 4: Send the email via Amazon SES
        send_email_via_ses(recipient_email, email_subject, email_body)
        
        # Step 5: Delete the processed message from the SQS queue
        sqs.delete_message(
            QueueUrl=QUEUE_URL,
            ReceiptHandle=message['ReceiptHandle']
        )

        return {
            'statusCode': 200,
            'body': json.dumps('Email sent successfully!')
        }

    except Exception as e:
        print(f"Error occurred: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps(f"Error: {str(e)}")
        }


def get_random_restaurants(cuisine, num_restaurants):
    """ Get num_restaurants random restaurant recommendations based on cuisine from DynamoDB """
    table = dynamodb.Table(DYNAMO_TABLE_NAME)
    
    # Scan DynamoDB to get restaurants by cuisine
    try:
        response = table.scan(
            FilterExpression="Cuisine = :cuisine",
            ExpressionAttributeValues={':cuisine': cuisine}
        )
        restaurants = response.get('Items', [])
        if not restaurants:
            return None

        # Select random restaurants
        if len(restaurants) > num_restaurants:
            selected_restaurants = random.sample(restaurants, num_restaurants)
        else:
            selected_restaurants = restaurants

        print(f"Selected restaurants: {selected_restaurants}")
        return selected_restaurants

    except Exception as e:
        print(f"Error fetching data from DynamoDB: {str(e)}")
        return None


def send_email_via_ses(recipient_email, subject, body):
    """ Send an email via Amazon SES """
    sender_email = "arcgupta148@gmail.com"  # The recipient email will also be the sender email in this case.
    try:
        response = ses.send_email(
            Source=sender_email,
            Destination={
                'ToAddresses': [recipient_email],
            },
            Message={
                'Subject': {
                    'Data': subject
                },
                'Body': {
                    'Text': {
                        'Data': body
                    }
                }
            }
        )
        print(f"Email sent! Message ID: {response['MessageId']}")
    except ClientError as e:
        print(f"Failed to send email. Error: {str(e)}")
