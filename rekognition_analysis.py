import boto3
import time
import logging
from botocore.exceptions import ClientError, BotoCoreError
from botocore.config import Config

# Initialize logging
logging.basicConfig(level=logging.INFO)

# Botocore config for retries and timeouts
config = Config(
    retries={
        'max_attempts': 10,
        'mode': 'standard'
    },
    connect_timeout=5,
    read_timeout=10
)

# Boto3 clients
rekognition_client = boto3.client('rekognition', config=config)
dynamodb = boto3.resource('dynamodb', config=config)
table = dynamodb.Table('VideoAnalysis')


def analyze_video(s3_key):
    """
    Initiates label detection on the video file stored in S3.
    """
    try:
        response = rekognition_client.start_label_detection(
            Video={'S3Object': {'Bucket': 'video-storage', 'Name': s3_key}},
            NotificationChannel={
                'RoleArn': 'arn:aws:iam::your-iam-role',
                'SNSTopicArn': 'arn:aws:sns:your-sns-topic'
            }
        )
        job_id = response['JobId']
        logging.info(f"Label detection started with Job ID: {job_id}")
        return job_id
    except ClientError as e:
        logging.error(f"Error starting label detection on video {s3_key}: {e}")
        raise
    except BotoCoreError as be:
        logging.error(f"BotoCore error: {be}")
        raise


def get_rekognition_results(job_id, poll_interval=15, max_attempts=20):
    """
    Polls for Rekognition label detection job results.
    Retries every `poll_interval` seconds until job completes or `max_attempts` is reached.
    """
    attempt = 0
    while attempt < max_attempts:
        try:
            response = rekognition_client.get_label_detection(JobId=job_id)
            status = response['JobStatus']

            if status == 'SUCCEEDED':
                logging.info(f"Rekognition label detection job {job_id} completed successfully.")
                return response
            elif status == 'FAILED':
                logging.error(f"Rekognition label detection job {job_id} failed.")
                raise Exception(f"Rekognition job {job_id} failed.")

            logging.info(f"Waiting for Rekognition job {job_id} to complete. Status: {status}")
            time.sleep(poll_interval)
            attempt += 1

        except ClientError as e:
            logging.error(f"Error fetching results for job {job_id}: {e}")
            raise
        except BotoCoreError as be:
            logging.error(f"BotoCore error: {be}")
            raise

    logging.error(f"Rekognition job {job_id} did not complete after {max_attempts * poll_interval} seconds.")
    raise TimeoutError(f"Rekognition job {job_id} timed out.")


def store_results_dynamodb(job_id, results, camera_id, location, confidence_threshold=80):
    """
    Stores Rekognition label detection results into DynamoDB.
    Filters results based on confidence threshold.

    Parameters:
    - job_id: Rekognition job ID.
    - results: Rekognition results containing detected labels.
    - camera_id: ID of the camera that captured the video.
    - location: Location of the camera.
    - confidence_threshold: Minimum confidence for storing detection results.
    """
    try:
        # Filter results by confidence threshold
        filtered_labels = [label for label in results['Labels'] if label['Confidence'] >= confidence_threshold]

        if not filtered_labels:
            logging.info(f"No labels above confidence threshold of {confidence_threshold}% for job {job_id}.")
            return

        # Prepare data for DynamoDB
        item = {
            'job_id': job_id,
            'camera_id': camera_id,
            'location': location,
            'results': filtered_labels,
            'timestamp': int(time.time())
        }

        # Store the results in DynamoDB
        table.put_item(Item=item)
        logging.info(f"Stored results for Job ID {job_id} in DynamoDB.")

    except ClientError as e:
        logging.error(f"Error storing Rekognition results for job {job_id} in DynamoDB: {e}")
        raise
    except BotoCoreError as be:
        logging.error(f"BotoCore error: {be}")
        raise
