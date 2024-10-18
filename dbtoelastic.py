import requests
import boto3
import json
from requests.auth import HTTPBasicAuth  # For basic authentication

# OpenSearch (Elasticsearch) domain details
ES_ENDPOINT = 'https://search-restaurant-domain-56t3dengujvnovrqxnkjtvreke.aos.us-east-1.on.aws'  # Replace with your OpenSearch endpoint
INDEX_NAME = 'restaurants'
TYPE_NAME = '_doc'

# Master User credentials for OpenSearch (Use the master user you created for the domain)
MASTER_USER = 'arcgupta148'
MASTER_PASSWORD = 'Mimo@148'

# AWS region and credentials
AWS_REGION = 'us-east-1'  # Replace with your actual region

# Hardcode your AWS credentials here (for testing purposes ONLY)
AWS_ACCESS_KEY_ID = 'AKIA2YICAICYHZNXB3NP'
AWS_SECRET_ACCESS_KEY = 'kw70OfzqXkm4dQF+XG+BxfyN2MeFLz9jKibXp8Fi'

# Initialize DynamoDB resource with AWS credentials
dynamodb = boto3.resource(
    'dynamodb',
    region_name=AWS_REGION,
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY
)
table = dynamodb.Table('yelp-restaurants')

# Function to get data from DynamoDB
def get_restaurants_from_dynamodb():
    try:
        response = table.scan()
        return response.get('Items', [])
    except Exception as e:
        print(f"Error fetching data from DynamoDB: {str(e)}")
        return []

# Function to push data to Elasticsearch (OpenSearch)
def push_to_elasticsearch(restaurant_id, cuisine):
    url = f"{ES_ENDPOINT}/{INDEX_NAME}/_doc/{restaurant_id}"
    headers = {'Content-Type': 'application/json'}
    
    data = {
        'RestaurantID': restaurant_id,
        'Cuisine': cuisine
    }
    
    try:
        # Add Basic Authentication using the master user credentials
        response = requests.put(url, headers=headers, data=json.dumps(data), auth=HTTPBasicAuth(MASTER_USER, MASTER_PASSWORD))
        
        if response.status_code == 201 or response.status_code == 200:
            print(f"Successfully indexed RestaurantID {restaurant_id}")
        else:
            print(f"Failed to index RestaurantID {restaurant_id}: {response.content}")
    except Exception as e:
        print(f"Error pushing to Elasticsearch: {str(e)}")

# Main function to push selected restaurant data to Elasticsearch
def index_restaurants_to_elasticsearch():
    restaurants = get_restaurants_from_dynamodb()
    count = 0
    max_records = 50 * 3  # 50 restaurants for each of 3 cuisines
    
    for restaurant in restaurants:
        if count >= max_records:
            break
        
        # Extract the required fields
        restaurant_id = restaurant.get('BusinessID')
        cuisine = restaurant.get('Cuisine')
        
        if restaurant_id and cuisine:
            push_to_elasticsearch(restaurant_id, cuisine)
            count += 1
        else:
            print(f"Skipping restaurant due to missing fields: {restaurant}")
    
    print(f"Finished indexing {count} restaurants to Elasticsearch.")

if __name__ == "__main__":
    index_restaurants_to_elasticsearch()
