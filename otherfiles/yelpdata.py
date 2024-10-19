import requests
import boto3
import json
from datetime import datetime
from time import sleep
from decimal import Decimal  # Import Decimal class

# Yelp API credentials
API_KEY = 'kBmAc9YFnGjiM4y8xk57VG84tcpApBNDqbcXXe678fQ-brRVoI_PABYMnMZ-I9dt_9EvxHh6suPrq1shdD1poht3EpkOjYAxltlrEjI0JRqHluFnT1-jAhNJrs0JZ3Yx'  # Replace with your Yelp API key
YELP_API_URL = 'https://api.yelp.com/v3/businesses/search'

AWS_ACCESS_KEY_ID = 'AKIA2YICAICYHZNXB3NP'
AWS_SECRET_ACCESS_KEY = 'kw70OfzqXkm4dQF+XG+BxfyN2MeFLz9jKibXp8Fi'
AWS_REGION = 'us-east-1'

session = boto3.Session(
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    region_name=AWS_REGION
)

# Initialize DynamoDB
dynamodb = session.resource('dynamodb')
table = dynamodb.Table('yelp-restaurants')  # Replace with your DynamoDB table name

# Function to query Yelp API
def query_yelp_api(term, location, offset):
    headers = {
        'Authorization': f'Bearer {API_KEY}'
    }
    params = {
        'term': term,
        'location': location,
        'limit': 50,  # Max Yelp allows per request
        'offset': offset  # For pagination
    }
    response = requests.get(YELP_API_URL, headers=headers, params=params)
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Failed to retrieve data from Yelp API: {response.status_code}")
        return None

# Function to store data into DynamoDB
def store_in_dynamodb(restaurant_data, cuisine):
    for restaurant in restaurant_data['businesses']:
        try:
            # Extract required fields from Yelp response and add 'Cuisine' to the item
            item = {
                'BusinessID': restaurant['id'],
                'Name': restaurant['name'],
                'Address': ', '.join(restaurant['location']['display_address']),
                'Coordinates': {
                    'Latitude': Decimal(str(restaurant['coordinates']['latitude'])),  # Convert float to Decimal
                    'Longitude': Decimal(str(restaurant['coordinates']['longitude']))  # Convert float to Decimal
                },
                'NumberOfReviews': restaurant.get('review_count', 0),
                'Rating': Decimal(str(restaurant.get('rating', 0.0))),  # Convert float to Decimal
                'ZipCode': restaurant['location'].get('zip_code', 'N/A'),
                'Cuisine': cuisine,  # Store the cuisine for each restaurant
                'InsertedAtTimestamp': str(datetime.now())
            }
            
            # Insert into DynamoDB
            table.put_item(Item=item)
            print(f"Inserted: {restaurant['name']} with cuisine {cuisine}")
        
        except Exception as e:
            print(f"Failed to insert restaurant: {restaurant['name']}. Error: {str(e)}")

# Main function to collect and store Yelp data
def scrape_and_store_yelp_data(cuisine, location):
    total_collected = 0
    offset = 0
    max_results = 100  # Max records to collect for this cuisine

    while total_collected < max_results:
        print(f"Querying Yelp for {cuisine} in {location} (Offset: {offset})")
        
        # Query Yelp API
        response_data = query_yelp_api(term=cuisine, location=location, offset=offset)
        
        # Break the loop if no data is returned
        if not response_data or 'businesses' not in response_data:
            break
        
        # Store data in DynamoDB, passing the 'cuisine' along
        store_in_dynamodb(response_data, cuisine)
        
        # Update counters and offset
        total_collected += len(response_data['businesses'])
        offset += 50  # Yelp API paginates in sets of 50 results
        
        # Sleep to avoid hitting API rate limits
        sleep(1)  # Adjust the sleep time based on the Yelp API rate limit

# Call the function to scrape data for multiple cuisines
if __name__ == "__main__":
    cuisines = ['Chinese', 'Italian', 'Japanese']
    location = 'Manhattan'
    
    for cuisine in cuisines:
        print(f"Scraping data for {cuisine} restaurants...")
        scrape_and_store_yelp_data(cuisine, location)
        print(f"Finished scraping data for {cuisine} restaurants.\n")
