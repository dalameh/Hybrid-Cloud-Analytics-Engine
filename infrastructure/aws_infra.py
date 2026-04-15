import aws_cdk as cdk
from aws_cdk import (
    aws_s3 as s3,
    aws_s3_notifications as s3n,
    aws_sns as sns,
    aws_iam as iam,
    aws_sns_subscriptions as subs,
    aws_sqs as sqs,
    Duration,
)
from constructs import Construct
from typing import cast

class OlistAWSInfrastructureStack(cdk.Stack):

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # 1. Dead-Letter Queue
        self.dlq = sqs.Queue(
            self, "OlistLandingDLQ",
            queue_name="olist-landing-dlq",
            retention_period=Duration.days(14),
            encryption=sqs.QueueEncryption.SQS_MANAGED,
        )

        # 2. Main SQS Queue
        self.landing_queue = sqs.Queue(
            self, "OlistLandingQueue",
            queue_name="olist-landing-queue",
            visibility_timeout=Duration.seconds(300),
            retention_period=Duration.days(4),
            encryption=sqs.QueueEncryption.SQS_MANAGED,
            dead_letter_queue=sqs.DeadLetterQueue(
                max_receive_count=3,
                queue=self.dlq,
            ),
        )

        # 3. SNS Topic
        topic = sns.Topic(
            self, "OlistLandingTopic",
            topic_name="olist-landing-topic",
            display_name="Olist Datalake Landing Events",
        )

        # 4. PERMISSION: Required for S3 to talk to SNS
        topic.grant_publish(iam.ServicePrincipal("s3.amazonaws.com"))

        # 5. SNS -> SQS
        topic.add_subscription(
            subs.SqsSubscription(self.landing_queue)
        )

        self.landing_topic = cast(sns.ITopic, topic)

        # 6. S3 Bucket
        self.datalake_bucket = s3.Bucket(
            self, "OlistDatalakeBucket",
            bucket_name=f"olist-ecommerce-landing-zone-useast1",
            versioned=True,
            encryption=s3.BucketEncryption.S3_MANAGED,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            removal_policy=cdk.RemovalPolicy.RETAIN,
            lifecycle_rules=[
                s3.LifecycleRule(
                    id="LandingTiering",
                    prefix="landing/",
                    transitions=[
                        s3.Transition(
                            storage_class=s3.StorageClass.INFREQUENT_ACCESS,
                            transition_after=Duration.days(30),
                        ),
                        s3.Transition(
                            storage_class=s3.StorageClass.GLACIER,
                            transition_after=Duration.days(90),
                        ),
                    ],
                    expiration=Duration.days(365),
                ),
            ],
        )

        self.lakehouse_bucket = s3.Bucket(
            self, "OlistLakehouseBucket",
            bucket_name=f"olist-ecommerce-prod-useast1-lakehouse",
            encryption=s3.BucketEncryption.S3_MANAGED,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            removal_policy=cdk.RemovalPolicy.RETAIN,
        )


        # 7. S3 Notification -> SNS
        self.datalake_bucket.add_event_notification( # all three are incredibly important to allow for managed file events
            s3.EventType.OBJECT_CREATED,
            s3.EventType.OBJECT_REMOVED,
            s3.EventType.LIFECYCLE_EXPIRATION,
            s3n.SnsDestination(self.landing_topic),
            s3.NotificationKeyFilter(prefix="landing/"),
        )

        # 8. Outputs
        cdk.CfnOutput(
            self, "SQSQueueUrl",
            value=self.landing_queue.queue_url,
            description="The URL of the SQS queue to be used in Databricks Auto Loader",
            export_name="OlistLandingQueueUrl"
        )

        cdk.CfnOutput(
            self, "SNSTopicArn",
            value=self.landing_topic.topic_arn,
            description="The ARN of the SNS topic to be used for Managed File Events registration",
            export_name="OlistLandingTopicArn"
        )

app = cdk.App()
OlistAWSInfrastructureStack(app, "Olist-AWS-Infrastructure-Stack")
app.synth()