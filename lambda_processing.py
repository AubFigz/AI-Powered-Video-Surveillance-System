import boto3
import cv2
import logging
import tempfile
import os
from botocore.exceptions import ClientError, BotoCoreError
from concurrent.futures import ThreadPoolExecutor
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

# Boto3 clients (reused across function calls)
s3_client = boto3.client('s3', config=config)
rekognition_client = boto3.client('rekognition', config=config)


def lambda_handler(event, context):
    try:
        # Extract video stream from event
        stream_arn = event.get('streamARN')
        if not stream_arn:
            raise ValueError("streamARN is required in the event.")

        logging.info(f"Processing video stream: {stream_arn}")

        # Retrieve video data from Kinesis
        media_client = boto3.client('kinesis-video-media', config=config)
        video_data = get_kinesis_video(media_client, stream_arn)

        if not video_data:
            raise ValueError("No video data retrieved from the Kinesis stream.")

        # Store raw video to S3 (optional, for long-term storage)
        s3_video_key = store_video_s3(video_data, "raw", context)

        # Extract frames from the video (configurable interval)
        frames = extract_frames(video_data, interval=1)  # 1 second by default

        # Preprocess frames (resize, compress, noise reduction)
        preprocessed_frames = preprocess_frames(frames)

        # Send preprocessed frames to Rekognition for analysis (parallel processing)
        analyze_frames_with_rekognition(preprocessed_frames, context)

    except ValueError as ve:
        logging.error(f"Input error: {ve}")
    except ClientError as ce:
        logging.error(f"AWS Client error: {ce}")
    except BotoCoreError as be:
        logging.error(f"BotoCore error: {be}")
    except Exception as e:
        logging.error(f"Unexpected error processing video stream: {e}")


def get_kinesis_video(media_client, stream_arn):
    """
    Retrieves video data from the Kinesis stream.
    """
    try:
        video_data = media_client.get_media(
            StreamARN=stream_arn,
            StartSelector={'StartSelectorType': 'NOW'}
        )
        logging.info(f"Successfully retrieved video from Kinesis stream: {stream_arn}")
        return video_data['Payload']  # Video payload to be processed
    except ClientError as e:
        logging.error(f"Error retrieving video from Kinesis stream: {e}")
        raise
    except BotoCoreError as be:
        logging.error(f"Error retrieving media: {be}")
        raise


def store_video_s3(video_data, prefix, context):
    """
    Store the raw video data to S3.
    """
    try:
        if not video_data:
            raise ValueError("No video data provided for S3 storage.")

        s3_key = f'{prefix}/{context.aws_request_id}.mp4'
        logging.info(f"Uploading video to S3 bucket with key {s3_key}...")

        s3_client.put_object(
            Bucket='processed-video-storage',
            Key=s3_key,
            Body=video_data,
            ServerSideEncryption='AES256'
        )
        logging.info(f"Stored video in S3: {s3_key}")
        return s3_key
    except ClientError as e:
        logging.error(f"Error storing video in S3: {e}")
        raise
    except BotoCoreError as be:
        logging.error(f"Error interacting with S3: {be}")
        raise


def extract_frames(video_data, interval=1):
    """
    Extracts frames from the video using OpenCV at a configurable interval.
    """
    try:
        with tempfile.NamedTemporaryFile(suffix='.mp4') as temp_video_file:
            temp_video_file.write(video_data.read())
            cap = cv2.VideoCapture(temp_video_file.name)

            if not cap.isOpened():
                raise ValueError("Unable to open video file for frame extraction.")

            frame_rate = cap.get(cv2.CAP_PROP_FPS)  # Frames per second
            frame_count = 0
            frames = []

            while True:
                ret, frame = cap.read()
                if not ret:
                    break

                # Extract frames at the specified interval (e.g., 1 frame per second)
                if frame_count % int(frame_rate * interval) == 0:
                    frames.append(frame)
                frame_count += 1

            cap.release()
            logging.info(f"Extracted {len(frames)} frames from video.")
            return frames
    except Exception as e:
        logging.error(f"Error extracting frames from video: {e}")
        raise


def preprocess_frames(frames):
    """
    Preprocess frames (resize, compress, noise reduction).
    """
    processed_frames = []
    try:
        for frame in frames:
            # Resize to 720p
            resized_frame = cv2.resize(frame, (1280, 720))

            # Apply Gaussian blur to reduce noise
            blurred_frame = cv2.GaussianBlur(resized_frame, (5, 5), 0)

            # Compress frame to JPEG format
            success, buffer = cv2.imencode('.jpg', blurred_frame, [int(cv2.IMWRITE_JPEG_QUALITY), 85])
            if success:
                processed_frames.append(buffer.tobytes())

        logging.info(f"Preprocessed {len(processed_frames)} frames.")
        return processed_frames
    except Exception as e:
        logging.error(f"Error preprocessing frames: {e}")
        raise


def analyze_frames_with_rekognition(frames, context):
    """
    Sends preprocessed frames to Amazon Rekognition for object detection in parallel.
    """

    def process_frame(frame, index):
        try:
            # Upload frame to S3 (Rekognition works with images in S3)
            frame_s3_key = f'frames/{index}_{context.aws_request_id}.jpg'
            s3_client.put_object(
                Bucket='processed-frames-storage',
                Key=frame_s3_key,
                Body=frame,
                ServerSideEncryption='AES256'
            )

            # Analyze the frame using Rekognition
            response = rekognition_client.detect_labels(
                Image={'S3Object': {'Bucket': 'processed-frames-storage', 'Name': frame_s3_key}},
                MaxLabels=10,
                MinConfidence=80
            )
            logging.info(f"Frame {index} Rekognition results: {response['Labels']}")
        except ClientError as e:
            logging.error(f"Error processing frame {index} with Rekognition: {e}")
        except BotoCoreError as be:
            logging.error(f"Error uploading frame to S3: {be}")

    # Process frames in parallel using ThreadPoolExecutor
    with ThreadPoolExecutor(max_workers=5) as executor:
        executor.map(process_frame, frames, range(len(frames)))
