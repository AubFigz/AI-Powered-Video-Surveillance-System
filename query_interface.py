import boto3
import logging
from botocore.exceptions import ClientError, BotoCoreError
from boto3.dynamodb.conditions import Key
from botocore.config import Config

# Initialize logging
logging.basicConfig(level=logging.INFO)

# Boto3 client configuration with retry logic
config = Config(
    retries={
        'max_attempts': 10,
        'mode': 'standard'
    },
    connect_timeout=5,
    read_timeout=10
)

# Boto3 clients
dynamodb = boto3.resource('dynamodb', config=config)
s3_client = boto3.client('s3', config=config)
table = dynamodb.Table('VideoAnalysis')


def lambda_handler(event, context):
    try:
        # Extract user ID from Cognito authorizer claims
        user_id = event['requestContext']['authorizer']['claims']['sub']
        logging.info(f"User {user_id} is querying for videos.")

        # Extract parameters from the Lex chatbot slots
        object_type = event['currentIntent']['slots']['Object']
        start_time = event['currentIntent']['slots']['StartTime']
        end_time = event['currentIntent']['slots']['EndTime']

        logging.info(f"Querying for object type: {object_type}, from {start_time} to {end_time}.")

        # Validate input parameters
        if not object_type or not start_time or not end_time:
            raise ValueError("Object type, start time, and end time are required.")

        # Query DynamoDB for matching video clips
        video_clips = query_dynamodb(user_id, object_type, start_time, end_time)

        if not video_clips:
            logging.info("No matching video clips found.")
            return generate_response("No matching video clips found for the given criteria.")

        # Generate presigned URLs for the video clips
        urls = [generate_presigned_url(clip['video_key']) for clip in video_clips]

        # Return the presigned URLs to the user
        return generate_response(f"Here are your video clips: {', '.join(urls)}")

    except ValueError as ve:
        logging.error(f"Input error: {ve}")
        return generate_response("Invalid input provided.")
    except ClientError as ce:
        logging.error(f"Error processing request: {ce}")
        return generate_response("An error occurred while processing your request.")
    except BotoCoreError as be:
        logging.error(f"BotoCore error: {be}")
        return generate_response("An error occurred while interacting with AWS services.")
    except Exception as e:
        logging.error(f"Unexpected error: {e}")
        return generate_response("An unexpected error occurred.")


def query_dynamodb(user_id, object_type, start_time, end_time):
    """
    Query DynamoDB for video clips based on user_id, object_type, and time range.
    """
    try:
        # Query DynamoDB based on user_id, object_type, and time range
        response = table.query(
            KeyConditionExpression=Key('user_id').eq(user_id) &
                                   Key('detected_objects').eq(object_type) &
                                   Key('timestamp').between(start_time, end_time)
        )
        logging.info(f"Found {len(response['Items'])} matching video clips.")
        return response['Items']
    except ClientError as e:
        logging.error(f"Error querying DynamoDB: {e}")
        raise
    except BotoCoreError as be:
        logging.error(f"BotoCore error: {be}")
        raise


def generate_presigned_url(video_key):
    """
    Generate a presigned URL for accessing the video clip in S3.
    """
    try:
        url = s3_client.generate_presigned_url(
            'get_object',
            Params={'Bucket': 'video-storage', 'Key': video_key},
            ExpiresIn=3600  # URL expiration time set to 1 hour
        )
        logging.info(f"Generated presigned URL for video: {video_key}")
        return url
    except ClientError as e:
        logging.error(f"Error generating presigned URL for {video_key}: {e}")
        raise
    except BotoCoreError as be:
        logging.error(f"BotoCore error: {be}")
        raise


def generate_response(message):
    """
    Helper function to generate the Lex chatbot response format.
    """
    return {
        "dialogAction": {
            "type": "Close",
            "fulfillmentState": "Fulfilled",
            "message": {"contentType": "PlainText", "content": message}
        }
    }
