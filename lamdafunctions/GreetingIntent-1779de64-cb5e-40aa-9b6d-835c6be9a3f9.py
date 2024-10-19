import json
import boto3

# Initialize the SQS client
sqs = boto3.client('sqs')
dynamodb = boto3.resource('dynamodb')

SQS_QUEUE_URL = 'https://sqs.us-east-1.amazonaws.com/739275456688/SQS_Q1'
DYNAMODB_TABLE_NAME = 'UserSearchHistory'  

def lambda_handler(event, context):
    # Log the full event to understand its structure
    print(f"Full event received from Lex: {json.dumps(event)}")
    
    # Safely attempt to get the intent name from sessionState
    intent_name = event.get('sessionState', {}).get('intent', {}).get('name', None)
    
    if intent_name is None:
        # If the intent name is missing, return a fallback response
        print("Intent name not found in event.")
        return close({}, "Sorry, I couldn't determine your intent. Please try again.", "FallbackIntent")

    session_attributes = event['sessionState'].get('sessionAttributes', {})
    user_id = event.get('sessionId')
    
    # Fetch previous search state from DynamoDB
    previous_search = get_previous_search(user_id)

    # Handle the GreetingIntent
    if intent_name == 'GreetingIntent':
        if previous_search:
            location = previous_search.get('LastLocation')
            category = previous_search.get('LastCategory')
            response_message = f"Welcome back! Last time, you searched for restaurants in {location} with cuisine type {category}. Would you like to continue with your previous search?"

            # Store the response for confirmation in session attributes
            session_attributes = event['sessionState'].get('sessionAttributes', {})
            session_attributes['isReturningUser'] = 'true'  # Mark the session as a returning user
            
            # Respond with the welcome back message
            response = close(session_attributes, response_message, intent_name)
        else:
            response = elicit_slot(event['sessionState'].get('sessionAttributes', {}), "DiningSuggestionsIntent", event['sessionState']['intent']['slots'], "Location", "Hi there, how can I help?")
        
        print(f"Response to Lex: {json.dumps(response)}")
        return response

    # Handle user confirmation
    if intent_name == 'ConfirmIntent' and event.get('sessionState', {}).get('sessionAttributes', {}).get('isReturningUser', 'false') == 'true':
        last_user_message = event.get('inputTranscript', '').lower()
        if last_user_message in ['yes', 'sure']:
            # Ask for how many people are in their party
            # Set slots with previous search information
            slots = {
                'Location': {'value': {'interpretedValue': previous_search.get('LastLocation', '')}},
                'Cuisine': {'value': {'interpretedValue': previous_search.get('LastCategory', '')}}
            }
            return elicit_slot(event['sessionState'].get('sessionAttributes', {}), "DiningSuggestionsIntent", slots, 'NumberOfPeople', "Great! How many people are in your party?")
        elif last_user_message in ['no', 'not now', 'no thanks']:
            # Respond to negative confirmation
            response_message = "No problem! How can I assist you today?"
            return elicit_slot(event['sessionState'].get('sessionAttributes', {}), "DiningSuggestionsIntent", event['sessionState']['intent']['slots'], "Location", response_message)
        else:
            # Handle unexpected responses
            response = close(event['sessionState'].get('sessionAttributes', {}), "Sorry, I didn't understand that response.", intent_name)
            print(f"Response to Lex: {json.dumps(response)}")
            return response

    # Handle other intents (ThankYouIntent, DiningSuggestionsIntent, etc.)
    if intent_name == 'ThankyouIntent':
        response = close(event['sessionState'].get('sessionAttributes', {}), "You’re welcome. Have a great day!", intent_name)
        print(f"Response to Lex: {json.dumps(response)}")
        return response

    if intent_name == 'DiningSuggestionsIntent':
        slots = event.get('sessionState', {}).get('intent', {}).get('slots', {})
        
        # Check for each slot and elicit the missing ones
        if not slots.get('Location'):
            return elicit_slot(event['sessionState'].get('sessionAttributes', {}), intent_name, slots, 'Location', 'Great. I can help you with that. What city or city area are you looking to dine in?')
        elif not slots.get('Cuisine'):
            return elicit_slot(event['sessionState'].get('sessionAttributes', {}), intent_name, slots, 'Cuisine', f"Got it, {slots['Location']['value']['interpretedValue']}. What cuisine would you like to try?")
        elif not slots.get('NumberOfPeople'):
            return elicit_slot(event['sessionState'].get('sessionAttributes', {}), intent_name, slots, 'NumberOfPeople', 'Ok, how many people are in your party?')
        elif not slots.get('DiningDate'):
            return elicit_slot(event['sessionState'].get('sessionAttributes', {}), intent_name, slots, 'DiningDate', 'A few more to go. What date are you looking to dine?')
        elif not slots.get('DiningTime'):
            return elicit_slot(event['sessionState'].get('sessionAttributes', {}), intent_name, slots, 'DiningTime', 'What time would you like to dine?')
        elif not slots.get('Email'):
            return elicit_slot(event['sessionState'].get('sessionAttributes', {}), intent_name, slots, 'Email', 'Great. Lastly, I need your email so I can send you the dining suggestions.')

        # Extract slot information
        location = slots.get('Location', {}).get('value', {}).get('interpretedValue', 'unknown-location')
        cuisine = slots.get('Cuisine', {}).get('value', {}).get('interpretedValue', 'unknown-cuisine')
        number_of_people = slots.get('NumberOfPeople', {}).get('value', {}).get('interpretedValue', 'unknown-number')
        dining_date = slots.get('DiningDate', {}).get('value', {}).get('interpretedValue', 'unknown-date')
        dining_time = slots.get('DiningTime', {}).get('value', {}).get('interpretedValue', 'unknown-time')
        email_value = slots.get('Email', {}).get('value', {}).get('interpretedValue', 'no-email@example.com')

        # Save user search state to DynamoDB
        save_user_search(user_id, location, cuisine)

        # Create a message payload for SQS
        message_body = {
            "Location": location,
            "Cuisine": cuisine,
            "NumberOfPeople": number_of_people,
            "DiningDate": dining_date,
            "DiningTime": dining_time,
            "Email": email_value
        }

        # Send the message to the SQS queue
        try:
            sqs_response = sqs.send_message(
                QueueUrl=SQS_QUEUE_URL,
                MessageBody=json.dumps(message_body)
            )
            print(f"Message sent to SQS with ID: {sqs_response['MessageId']}")
        except Exception as e:
            print(f"Failed to send message to SQS: {str(e)}")

        response = close(event['sessionState'].get('sessionAttributes', {}), 
                        f"You’re all set. Expect my suggestions shortly! I will send them to {email_value}. Have a good day.", 
                        intent_name)
        print(f"Response to Lex: {json.dumps(response)}")
        return response

    # Default fallback message
    response = close(event['sessionState'].get('sessionAttributes', {}), "Sorry, I don't understand that request.", intent_name)
    print(f"Response to Lex: {json.dumps(response)}")
    return response


# Function to fetch previous search state from DynamoDB
def get_previous_search(user_id):
    table = dynamodb.Table(DYNAMODB_TABLE_NAME)
    try:
        response = table.get_item(Key={'UserID': user_id})
        return response.get('Item', {})
    except Exception as e:
        print(f"Error fetching previous search: {str(e)}")
        return {}

# Function to save user search state to DynamoDB
def save_user_search(user_id, location, cuisine):
    print(f"Entered to save the id in dynamo db")
    table = dynamodb.Table(DYNAMODB_TABLE_NAME)
    try:
        table.put_item(
            Item={
                'UserID': user_id,
                'LastLocation': location,
                'LastCategory': cuisine
            }
        )
        print(f"User search state saved for {user_id}.")
    except Exception as e:
        print(f"Error saving user search state: {str(e)}")

# Helper function to format the Lex response for closing the intent
def close(session_attributes, message, intent_name):
    return {
        "sessionState": {
            "sessionAttributes": session_attributes,
            "dialogAction": {
                "type": "Close",
                "fulfillmentState": "Fulfilled"
            },
            "intent": {
                "name": intent_name,
                "state": "Fulfilled"
            }
        },
        "messages": [
            {
                "contentType": "PlainText",
                "content": message
            }
        ]
    }

# Helper function to elicit missing slot information
def elicit_slot(session_attributes, intent_name, slots, slot_to_elicit, message):
    response = {
        "sessionState": {
            "sessionAttributes": session_attributes,
            "dialogAction": {
                "type": "ElicitSlot",
                "slotToElicit": slot_to_elicit,
            },
            "intent": {
                "name": intent_name,
                "slots": slots  # Carry over the slots to retain the filled information
            }
        },
        "messages": [
            {
                "contentType": "PlainText",
                "content": message
            }
        ]
    }
    return response
