from aws_cdk import (
    aws_lambda as _lambda,
    core
)

from aws_cdk.custom_resources import AwsCustomResource, AwsCustomResourcePolicy, AwsSdkCall
from constructs import Construct
import json

class RDSInitializer(Construct):
    function = None

    def __init__(self, scope: Construct, id: str, **kwargs) -> None:
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
        )

        # payload = json.loads({
        #     'params': {
        #         'config': props.config
        #     }
        # })
        sdk_call = AwsSdkCall(
                service='Lambda',
                action='invoke',
            )

        AwsCustomResource(self, "RDSInitializerResource", 
            function_name=fn.function_name,
            policy=AwsCustomResourcePolicy.from_sdk_calls(
                resources=AwsCustomResourcePolicy.ANY_RESOURCE
            ),
            on_create=sdk_call
        )

        self.function = fn