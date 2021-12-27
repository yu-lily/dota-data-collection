from aws_cdk import core as cdk

from aws_cdk import (
    aws_lambda as _lambda,
    aws_ec2 as ec2,
    aws_rds as rds,
    core,
)
from aws_cdk.custom_resources import AwsCustomResource, AwsCustomResourcePolicy, AwsSdkCall, PhysicalResourceId
from constructs import Construct
import json

from dataclasses import dataclass

@dataclass
class RDSInitializerProps:
    vpc: ec2.Vpc
    rds_creds: rds.Credentials
    db_endpoint: rds.DatabaseProxy

class RDSInitializer(cdk.Construct):

    def __init__(self, scope: Construct, id: str, props: RDSInitializerProps, **kwargs) -> None:
        super().__init__(scope, id)

        LAMBDA_ENVS = {
            "rds_creds": props.rds_creds.secret_name,
            "db_endpoint": props.db_endpoint.to_string(),
        }
        print(LAMBDA_ENVS)
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
            environment=LAMBDA_ENVS,
            timeout=core.Duration.minutes(1),
            profiling=True,
            vpc = props.vpc,
        )

        sdk_call = AwsSdkCall(
                service='Lambda',
                action='invoke',
                parameters={
                    'FunctionName': fn.function_name,
                },
                physical_resource_id=PhysicalResourceId.of(f"{id}-{fn.function_name}-sdkCall"),
            )


        AwsCustomResource(self, "RDSInitializerResource", 
            function_name=fn.function_name,
            policy=AwsCustomResourcePolicy.from_sdk_calls(
                resources=AwsCustomResourcePolicy.ANY_RESOURCE
            ),
            on_create=sdk_call
        )

        self.function = fn

    def get_function(self):
        return self.function