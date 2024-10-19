import json
import boto3
import uuid

# Initialize the Lex V2 client
lex_client = boto3.client('lexv2-runtime')

# Bot and Alias details
BOT_ID = 'CMICPVTHDS'          # Your Bot ID
BOT_ALIAS_ID = 'TSTALIASID'     # Your Bot Alias ID
LOCALE_ID = 'en_US'             # Your Locale ID

def lambda_handler(event, context):
    # Log the incoming event to help debug the input
    print(f"Incoming event: {json.dumps(event)}")
    
    try:
        user_input = event['messages'][0]['unstructured']['text']
    except KeyError:
        # If the expected keys are missing, log an error and default to "Hello"
        print("Error: Could not extract 'text' from event. Defaulting to 'Hello'.")
        user_input = 'Hello'
    
    # Log the user input to ensure it's captured correctly
    print(f"User input: {user_input}")
    
    # Get the session ID from the event, or create a new one if not provided
    session_id = "he1" #event.get('sessionId', str(uuid.uuid4()))  # Generates a new session ID if none is provided

    # Log the session ID to ensure it's consistent
    print(f"Using session ID: {session_id}")
    
    try:
        # Log the request details before calling Lex
        print(f"Calling Lex with BOT_ID: {BOT_ID}, BOT_ALIAS_ID: {BOT_ALIAS_ID}, LOCALE_ID: {LOCALE_ID}, sessionId: {session_id}, text: {user_input}")

        # Call the Lex bot using recognize_text
        lex_response = lex_client.recognize_text(
            botId=BOT_ID,
            botAliasId=BOT_ALIAS_ID,
            localeId=LOCALE_ID,
            sessionId=session_id,  # Use the dynamic session ID
            text=user_input
        )

        # Log the raw response from Lex
        print(f"Lex response: {json.dumps(lex_response)}")

        # Extract the bot's message from the Lex response
        messages = lex_response.get('messages', [])
        
        # Log how many messages were received from Lex
        print(f"Number of messages received from Lex: {len(messages)}")

        if messages:
            # Log the content of the first message
            print(f"First message content: {messages[0].get('content')}")
            bot_reply = messages[0].get('content', 'Sorry, I could not understand that.')
        else:
            # Log when no messages are received from Lex
            print("No messages received from Lex.")
            bot_reply = "Sorry, I didn't receive a response from Lex."

        # Log the final bot reply before returning it
        print(f"Bot reply: {bot_reply}")

        # Return the bot's response in the Lambda response body
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': bot_reply,
                'sessionId': session_id  # Return the session ID in the response so it can be reused
            })
        }

    except Exception as e:
        # Log the error if something goes wrong
        print(f"Error calling Lex: {str(e)}")
        
        # Return the error message in the Lambda response body
        return {
            'statusCode': 500,
            'body': json.dumps({
                'error': str(e),
                'message': 'An error occurred while processing your request.'
            })
        }
