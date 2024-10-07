import boto3
import logging
from botocore.exceptions import ClientError, BotoCoreError
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

# Boto3 CloudWatch client
cloudwatch_client = boto3.client('cloudwatch', config=config)


def log_metrics(metric_name, value, unit):
    """
    Log custom metrics to AWS CloudWatch.

    Parameters:
    - metric_name (str): The name of the metric to log.
    - value (float): The value of the metric.
    - unit (str): The unit of the metric (e.g., 'Count', 'Milliseconds').
    """
    try:
        response = cloudwatch_client.put_metric_data(
            Namespace='VideoSurveillance',
            MetricData=[{
                'MetricName': metric_name,
                'Value': value,
                'Unit': unit
            }]
        )
        logging.info(f"Successfully logged metric '{metric_name}' with value: {value} {unit}")
        return response
    except ClientError as e:
        logging.error(f"Error logging metric '{metric_name}': {e}")
        raise
    except BotoCoreError as be:
        logging.error(f"BotoCore error logging metric '{metric_name}': {be}")
        raise


def create_cloudwatch_alarm(metric_name, threshold, comparison_operator='GreaterThanThreshold', evaluation_periods=1,
                            period=300, sns_topic_arn=None):
    """
    Create a CloudWatch alarm based on the given metric.

    Parameters:
    - metric_name (str): The name of the metric to create an alarm for.
    - threshold (float): The threshold value for the alarm.
    - comparison_operator (str): The comparison operator (e.g., 'GreaterThanThreshold').
    - evaluation_periods (int): The number of evaluation periods before the alarm triggers.
    - period (int): The evaluation period in seconds (default is 300 seconds or 5 minutes).
    - sns_topic_arn (str): Optional SNS topic ARN to notify when the alarm is triggered.
    """
    try:
        alarm_name = f'{metric_name}_Alarm'
        alarm_actions = [sns_topic_arn] if sns_topic_arn else []

        response = cloudwatch_client.put_metric_alarm(
            AlarmName=alarm_name,
            MetricName=metric_name,
            Namespace='VideoSurveillance',
            Threshold=threshold,
            ComparisonOperator=comparison_operator,
            EvaluationPeriods=evaluation_periods,
            Period=period,
            Statistic='Average',  # Adjust statistic type if necessary (Sum, Maximum, Minimum, etc.)
            ActionsEnabled=True,
            AlarmActions=alarm_actions,
            AlarmDescription=f"Alarm when {metric_name} exceeds {threshold}",
            Unit='None'  # You can specify unit if needed, but 'None' is often applicable
        )
        logging.info(f"Successfully created CloudWatch alarm: {alarm_name}")
        return response
    except ClientError as e:
        logging.error(f"Error creating CloudWatch alarm for metric '{metric_name}': {e}")
        raise
    except BotoCoreError as be:
        logging.error(f"BotoCore error creating alarm '{metric_name}': {be}")
        raise
