Project Title: AI-Powered Video Surveillance System
Project Description
This project is an AI-powered video surveillance system built using AWS services. It captures live video streams from cameras, processes the video frames for object detection using Amazon Rekognition, and stores the results in DynamoDB. Users can query and retrieve specific video clips through an integrated chatbot interface. The system is scalable, event-driven, and built with security in mind, leveraging AWS services such as Kinesis Video Streams, Lambda, S3, DynamoDB, CloudWatch, and SNS for alerts.

Contents
The repository contains the following Python scripts and a project PDF that describes the entire architecture and implementation in detail:

kinesis_ingestion.py
lambda_processing.py
rekognition_analysis.py
query_interface.py
cloudwatch_logging.py
AWS Powered Video Surveillance.pdf
Files Overview
1. kinesis_ingestion.py
This script is responsible for the ingestion of video streams from cameras into AWS Kinesis Video Streams. It attaches important metadata such as camera ID, location, and video resolution. The video stream is then stored in an S3 bucket for long-term storage.

Key Features:

Creates a Kinesis video stream and stores metadata.
Stores raw video data in Amazon S3 with encryption for security.
Metadata includes camera ID, location, resolution, frame rate, and video format.
External Requirements:

Requires configuration of Kinesis Video Streams and the S3 bucket in AWS.
Key Functions:

ingest_video_stream(camera_id, location, resolution, frame_rate, video_format): Ingests the video into Kinesis with metadata tags.
store_video_s3(video_data, camera_id): Stores the video data in S3 for archival purposes.
2. lambda_processing.py
This Lambda function is triggered by Kinesis Video Streams to process the video frames. It extracts frames from the video, compresses them, and sends them to Amazon Rekognition for object detection.

Key Features:

Extracts video frames from the Kinesis stream.
Preprocesses frames by resizing, compressing, and noise reduction.
Sends the preprocessed frames to Amazon Rekognition for object detection.
External Requirements:

Requires integration with Amazon Rekognition and OpenCV for frame processing.
Key Functions:

lambda_handler(event, context): Main handler that retrieves the video stream and processes it.
extract_frames(video_data, interval=1): Extracts video frames at specified intervals.
analyze_frames_with_rekognition(frames, context): Sends frames to Rekognition for object detection.
3. rekognition_analysis.py
This script analyzes video frames using Amazon Rekognition. It initiates the object detection process on video files stored in S3 and stores the results in DynamoDB.

Key Features:

Initiates object detection jobs using Amazon Rekognition.
Polls for job completion and retrieves the analysis results.
Stores detected objects and metadata in DynamoDB for querying.
External Requirements:

Requires an S3 bucket to store videos and a DynamoDB table to store detection results.
Key Functions:

analyze_video(s3_key): Initiates a Rekognition label detection job.
get_rekognition_results(job_id): Polls for the completion of the detection job.
store_results_dynamodb(job_id, results, camera_id, location): Stores the analysis results in DynamoDB.
4. query_interface.py
This script provides an interface to query the detected objects in the video streams. Users can use a Lex chatbot interface to query video clips based on object types, time range, or camera ID. It returns presigned URLs for the requested video clips stored in S3.

Key Features:
Queries the DynamoDB table for detected objects in a specified time range.
Generates presigned URLs to provide temporary access to video clips stored in S3.
External Requirements:
Requires configuration with Amazon Lex and a DynamoDB table to store detection results.
Key Functions:
query_dynamodb(user_id, object_type, start_time, end_time): Queries DynamoDB for relevant video clips.
generate_presigned_url(video_key): Generates a temporary presigned URL for video access.
lambda_handler(event, context): Main handler that interacts with Lex and DynamoDB.
5. cloudwatch_logging.py
This script logs custom metrics to AWS CloudWatch and sets up alarms based on those metrics. It is useful for monitoring the system’s performance and alerting on specific thresholds, such as failed jobs or high processing times.

Key Features:

Logs custom metrics such as frame processing time, Rekognition API response time.
Creates CloudWatch alarms that trigger based on thresholds.
External Requirements:

Requires configuration of CloudWatch metrics and alarms in AWS.
Key Functions:

log_metrics(metric_name, value, unit): Logs metrics to CloudWatch.
create_cloudwatch_alarm(metric_name, threshold, comparison_operator): Creates alarms to monitor system performance.
6. AWS Powered Video Surveillance.pdf
This document describes the project’s architecture and workflow in detail, outlining how the system was designed, the AWS services used, and how the different components interact. It provides an overview of all the scripts, key steps, and additional configuration details necessary to deploy the project in a real-world environment.

How to Run
Prerequisites
AWS Account with appropriate permissions to manage Kinesis Video Streams, S3, Lambda, Rekognition, DynamoDB, CloudWatch, and SNS.
Boto3 and OpenCV installed in your local Python environment for testing (if necessary).
Steps
Set Up AWS Services:

Create and configure Kinesis Video Streams, S3, Lambda, DynamoDB, and CloudWatch.
Ensure IAM Roles are in place with appropriate permissions.
Deploy Lambda Functions:

Upload and configure the Lambda scripts (lambda_processing.py, rekognition_analysis.py, query_interface.py) in AWS Lambda.
Set up event triggers for Kinesis Video Streams and S3 for the Lambda functions.
Configure DynamoDB and CloudWatch:

Create a DynamoDB table for storing video metadata and object detection results.
Configure CloudWatch for logging and creating alarms based on system performance.
Test System:

Ingest video streams using kinesis_ingestion.py.
Ensure frames are processed and stored in S3, and object detection results are stored in DynamoDB.
Use the chatbot or query functions in query_interface.py to retrieve presigned URLs for video clips.
