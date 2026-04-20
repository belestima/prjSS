from aws_cdk import (
    Duration,
    Stack,
    RemovalPolicy,
    aws_s3 as s3,
    aws_lambda as _lambda,
    aws_lambda_event_sources as lambda_event_sources,
    aws_iam as iam,
)
from constructs import Construct

class CdkStack(Stack):

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # Create S3 bucket
        bucket = s3.Bucket(
            self, "MyBucket",
            bucket_name="bucket-ss",  
            removal_policy=RemovalPolicy.DESTROY,  # For development only
        )

        # Create Lambda function
        lambda_function = _lambda.Function(
            self, "S3EventProcessor",
            runtime=_lambda.Runtime.PYTHON_3_9,
            code=_lambda.Code.from_asset("../src"),
            handler="main.lambda_handler",
            timeout=Duration.seconds(30),
        )

        # Grant Lambda permission to read from S3
        bucket.grant_read(lambda_function)

        # Add S3 event source to Lambda
        lambda_function.add_event_source(
            lambda_event_sources.S3EventSource(
                bucket,
                events=[s3.EventType.OBJECT_CREATED],
                filters=[s3.NotificationKeyFilter(prefix="input/")],  # Optional: only trigger for files in input/ folder
            )
        )

        # Bucket policy (additional permissions if needed)
        # The grant_read above should suffice, but if more specific policy needed:
        bucket_policy = iam.PolicyStatement(
            actions=["s3:GetObject"],
            resources=[bucket.bucket_arn + "/*"],
            principals=[lambda_function.role],
        )
        bucket.add_to_resource_policy(bucket_policy)
