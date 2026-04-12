import aws_cdk as cdk
from aws_cdk import (
    aws_s3 as s3,
    Duration,
)
from constructs import Construct

class OlistDatalakeInfaStack(cdk.Stack):

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # Create S3 bucket for data lake
        self.datalake_bucket = s3.Bucket(self, "OlistDatalakeBucket",
            bucket_name="olist-ecommerce-landing-bucket",
            versioned=True,
            encryption = s3.BucketEncryption.S3_MANAGED,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            removal_policy=cdk.RemovalPolicy.RETAIN,

            # Standard to IA after 30 days, 
            # and expire non-current versions after 30 days
            lifecycle_rules=[
                s3.LifecycleRule(
                    id="LandingIA",
                    prefix="landing/",
                    transitions=[
                        s3.Transition(
                            storage_class=s3.StorageClass.INFREQUENT_ACCESS,
                            transition_after=Duration.days(30)
                        )
                    ],
                    noncurrent_version_expiration=Duration.days(30)
                ),

                # # Move to Intelligent Tiering immediately for Gold layer,
                # # as it may have variable access patterns
                # s3.LifecycleRule(
                #     id="GoldIT",
                #     prefix="04_gold/",
                #     transitions=[
                #         s3.Transition(
                #             storage_class=s3.StorageClass.INTELLIGENT_TIERING,
                #             transition_after=Duration.days(0)
                #         )
                #     ]
                # )
            ]
        )

# INITALIZE APP
app = cdk.App()
OlistDatalakeInfaStack(app, "Databricks-Olist-Datalake-Stack")
app.synth()