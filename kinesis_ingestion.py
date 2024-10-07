import boto3
import datetime
import logging
from botocore.config import Config
from botocore.exceptions import ClientError, BotoCoreError

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

# AWS Kinesis Video Streams client (global client to be reused)
kinesis_client = boto3.client('kinesisvideo', config=config)

# AWS S3 client (global client to be reused)
s3_client = boto3.client('s3', config=config)


def ingest_video_stream(camera_id, location, resolution, frame_rate, video_format):
    """
    Ingests video stream into Kinesis Video Streams with embedded metadata.

    Parameters:
    camera_id: Unique identifier for the camera.
    location: Physical location of the camera.
    resolution: Resolution of the video (e.g., '1080p', '720p').
    frame_rate: Frame rate of the video (e.g., '30fps').
    video_format: Video format (e.g., 'H.264').
    """
    try:
        if not camera_id or not location or not resolution or not frame_rate or not video_format:
            raise ValueError(
                "All parameters (camera_id, location, resolution, frame_rate, video_format) must be provided.")

        # Metadata and stream information
        metadata = {
            'camera_id': camera_id,
            'location': location,
            'resolution': resolution,
            'frame_rate': frame_rate,
            'video_format': video_format,
            'timestamp': datetime.datetime.now().isoformat()
        }

        logging.info(f"Attempting to create Kinesis stream for camera {camera_id} at {location}...")

        # Create the Kinesis stream with metadata tags
        stream_arn = kinesis_client.create_stream(
            StreamName=f'video-stream-{camera_id}',
            MediaType=video_format
        )['StreamARN']

        logging.info(f'Stream created with ARN: {stream_arn}')

        # Attach metadata to the stream (embedded tags)
        kinesis_client.tag_stream(
            StreamARN=stream_arn,
            Tags=metadata
        )
        logging.info(f'Metadata tags added: {metadata}')

        return stream_arn

    except ValueError as ve:
        logging.error(f'Input error: {ve}')
        return None
    except ClientError as ce:
        logging.error(f'AWS Client error while creating Kinesis stream: {ce}')
        return None
    except BotoCoreError as be:
        logging.error(f'BotoCore error while interacting with AWS services: {be}')
        return None
    except Exception as e:
        logging.error(f'Unexpected error creating Kinesis video stream: {e}')
        return None


def store_video_s3(video_data, camera_id):
    """
    Stores video data into S3 bucket.

    Parameters:
    video_data: The actual video data to be stored.
    camera_id: Unique identifier for the camera.
    """
    try:
        if not video_data or not camera_id:
            raise ValueError("Both video_data and camera_id must be provided.")

        # Generate unique S3 key using camera_id and timestamp
        s3_key = f'videos/{camera_id}/{datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")}.mp4'

        logging.info(f"Uploading video to S3 bucket with key {s3_key}...")

        # Upload video to S3 with default encryption (SSE-S3)
        s3_client.put_object(
            Bucket='video-storage',
            Key=s3_key,
            Body=video_data,
            ServerSideEncryption='AES256'  # Ensures data is encrypted at rest
        )

        logging.info(f'Video data stored in S3 with key: {s3_key}')

    except ValueError as ve:
        logging.error(f'Input error: {ve}')
    except ClientError as ce:
        logging.error(f'AWS Client error while uploading video to S3: {ce}')
    except BotoCoreError as be:
        logging.error(f'BotoCore error while interacting with AWS services: {be}')
    except Exception as e:
        logging.error(f'Unexpected error storing video in S3: {e}')
