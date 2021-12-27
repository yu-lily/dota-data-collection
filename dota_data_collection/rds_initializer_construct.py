from aws_cdk import (
    aws_lambda as _lambda,
    aws_ec2 as ec2,
    aws_rds as rds,
    core
)

from aws_cdk.custom_resources import AwsCustomResource, AwsCustomResourcePolicy, AwsSdkCall, PhysicalResourceId, Provider
from constructs import Construct
import json

from dataclasses import dataclass

@dataclass
class RDSInitializerProps:
    vpc: ec2.Vpc
    rds_creds: rds.Credentials

class RDSInitializer(Construct):

    def __init__(self, scope: Construct, id: str, props: RDSInitializerProps, **kwargs) -> None:
        super().__init__(scope, id)

        fn = _lambda.Function(self, "RDSInitializerProvider",
            runtime=_lambda.Runtime.PYTHON_3_8,
            handler="index.handler",
            code=_lambda.Code.from_asset(
                "lambda/rds_initializer",
                bundling=core.BundlingOptions(
                    image=_lambda.Runtime.PYTHON_3_8.bundling_image,
                    command=[
                        "bash", "-c",
                        "pip install --no-cache -r requirements.txt -t /asset-output && cp -au . /asset-output"
                    ],
                ),
            ),
            timeout=core.Duration.minutes(1),
            profiling=True,
            vpc = props.vpc,
        )

        sdk_call = AwsSdkCall(
                service='Lambda',
                action='invoke',
                # parameters={
                #     'FunctionName': fn.function_arn,
                # },
                physical_resource_id=PhysicalResourceId.of("id")
            )

        # provider = Provider(self, "RDSInitializerProvider",
        #     on_event_handler=fn
        # )


        AwsCustomResource(self, "RDSInitializerResource", 
            #function_name=fn.function_name,
            policy=AwsCustomResourcePolicy.from_sdk_calls(
                resources=AwsCustomResourcePolicy.ANY_RESOURCE
            ),
            on_create=sdk_call
        )

        self.function = fn

    def get_function(self):
        return self.function