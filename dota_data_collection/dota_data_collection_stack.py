from aws_cdk import core as cdk

from aws_cdk import (
    aws_dynamodb as ddb,
    aws_lambda as _lambda,
    aws_sqs as sqs,
    aws_events as events,
    #aws_lambda_event_sources as event_sources,
    aws_secretsmanager as sm,
    aws_iam as iam,
    core
)
from aws_cdk.aws_lambda_event_sources import SqsEventSource

class DotaDataCollectionStack(cdk.Stack):

    def __init__(self, scope: cdk.Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # Handle Stratz API Key
        stratz_apikey = sm.Secret.from_secret_name_v2(self, "StratzAPIKey", secret_name="stratz/apikey")
        stratz_api_caller = iam.Role(self, 'StratzAPICaller',
            assumed_by=iam.ServicePrincipal('lambda.amazonaws.com'))

        # Create SQS Queue
        staging_queue = sqs.Queue(self, 'StagingQueue',
            visibility_timeout=core.Duration.seconds(300),
        )
        api_caller_queue = sqs.Queue(self, 'APICallerQueue',
            visibility_timeout=core.Duration.seconds(120),
        )

        # Create a DynamoDB table
        players_table = ddb.Table(
            self, "PlayersTable",
            partition_key=ddb.Attribute(name="player_id", type=ddb.AttributeType.NUMBER),
            billing_mode=ddb.BillingMode.PAY_PER_REQUEST,
            removal_policy=core.RemovalPolicy.DESTROY
        )
        matchid_table = ddb.Table(
            self, "MatchIDTable",
            partition_key=ddb.Attribute(name="match_id", type=ddb.AttributeType.NUMBER),
            removal_policy=core.RemovalPolicy.DESTROY
        )
        api_calls_table = ddb.Table(
            self, "APICallsTable",
            partition_key=ddb.Attribute(name="api_call_id", type=ddb.AttributeType.NUMBER),
            billing_mode=ddb.BillingMode.PAY_PER_REQUEST,
            removal_policy=core.RemovalPolicy.DESTROY
        )
        aghanim_matches_table = ddb.Table(
            self, "AghanimMatchesTable",
            partition_key=ddb.Attribute(name="id", type=ddb.AttributeType.NUMBER),
            billing_mode=ddb.BillingMode.PAY_PER_REQUEST,
            removal_policy=core.RemovalPolicy.DESTROY,
            point_in_time_recovery=True,
        )
        aghanim_query_window_table = ddb.Table(
            self, "AghanimQueryWindowTable",
            partition_key=ddb.Attribute(name="start_time", type=ddb.AttributeType.NUMBER),
            sort_key=ddb.Attribute(name="difficulty", type=ddb.AttributeType.STRING),
            billing_mode=ddb.BillingMode.PAY_PER_REQUEST,
            removal_policy=core.RemovalPolicy.DESTROY,
        )

        # Lambda Functions
        api_caller_layer = _lambda.LayerVersion(
            self, "APICallerLayer",
            code=_lambda.Code.from_asset("api_caller_layer"),
            compatible_runtimes=[
                _lambda.Runtime.PYTHON_3_7,
                _lambda.Runtime.PYTHON_3_8
            ]
        )

        LAMBDA_ENVS = {
            "PLAYERS_TABLE": players_table.table_name,
            "MATCHID_TABLE": matchid_table.table_name,
            "API_CALLS_TABLE": api_calls_table.table_name,
            "AGHANIM_MATCHES_TABLE": aghanim_matches_table.table_name,
            "AGHANIM_QUERY_WINDOW_TABLE": aghanim_query_window_table.table_name,
            "STAGING_QUEUE": staging_queue.queue_name,
            "API_CALLER_QUEUE": api_caller_queue.queue_name,
            "PARTITION_KEY": "player_id",
        }

        api_caller_lambda = _lambda.Function(
            self, "APICaller",
            runtime=_lambda.Runtime.PYTHON_3_8,
            handler="index.handler",
            code=_lambda.Code.from_asset(
                "lambda/api_caller",
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
            layers=[api_caller_layer],
        )


        aghanim_matches_lambda = _lambda.Function(
            self, "AghanimMatchesLambda",
            runtime=_lambda.Runtime.PYTHON_3_8,
            handler='aghanim_matches.handler',
            code=_lambda.Code.from_asset(
                "lambda/aghanim_matches",
                bundling=core.BundlingOptions(
                    image=_lambda.Runtime.PYTHON_3_8.bundling_image,
                    command=[
                        "bash", "-c",
                        "pip install --no-cache -r requirements.txt -t /asset-output && cp -au . /asset-output"
                    ],
                ),
            ),
            environment=LAMBDA_ENVS,
            timeout=core.Duration.minutes(10),
            profiling=True,
            layers=[api_caller_layer],
        )

        LAMBDA_FUNC_NAMES = {
            "AGHANIM_MATCHES_FUNC_NAME": aghanim_matches_lambda.function_name,
            "API_CALLER_FUNC_NAME": api_caller_lambda.function_name,
        }

        orchestrator_lambda = _lambda.Function(
            self, "OrchestratorLambda",
            runtime=_lambda.Runtime.PYTHON_3_8,
            handler='orchestrator.handler',
            code=_lambda.Code.from_asset(
                "lambda/orchestrator",
                bundling=core.BundlingOptions(
                    image=_lambda.Runtime.PYTHON_3_8.bundling_image,
                    command=[
                        "bash", "-c",
                        "pip install --no-cache -r requirements.txt -t /asset-output && cp -au . /asset-output"
                    ],
                ),
            ),
            environment={**LAMBDA_ENVS, **LAMBDA_FUNC_NAMES},
            timeout=core.Duration.minutes(10),
            profiling=True,
        )

        queue_populator_lambda = _lambda.Function(
            self, "QueuePopulatorLambda",
            runtime=_lambda.Runtime.PYTHON_3_8,
            handler='queue_populator.handler',
            code=_lambda.Code.from_asset(
                "lambda/orchestrator",
                bundling=core.BundlingOptions(
                    image=_lambda.Runtime.PYTHON_3_8.bundling_image,
                    command=[
                        "bash", "-c",
                        "pip install --no-cache -r requirements.txt -t /asset-output && cp -au . /asset-output"
                    ],
                ),
            ),
            environment={**LAMBDA_ENVS, **LAMBDA_FUNC_NAMES},
            timeout=core.Duration.minutes(10),
            profiling=True,
        )

        api_caller_lambda.add_event_source(SqsEventSource(api_caller_queue,
            batch_size=1,
        ))

        #STRATZ API KEY ACCESS
        #Give Stratz API Key access to functions that call the API
        stratz_apikey.grant_read(aghanim_matches_lambda)
        stratz_apikey.grant_read(api_caller_lambda)

        #QUEUE PERMISSIONS
        #Give the queues access to the functions that call the API
        staging_queue.grant_consume_messages(orchestrator_lambda)
        staging_queue.grant_send_messages(orchestrator_lambda)
        api_caller_queue.grant_send_messages(orchestrator_lambda)
        api_caller_queue.grant_consume_messages(api_caller_lambda)
        staging_queue.grant_send_messages(api_caller_lambda)
        staging_queue.grant_send_messages(queue_populator_lambda)

        #ALLOW FUNCTION CALLS
        #Allow the orchestrator function to call other functions
        aghanim_matches_lambda.grant_invoke(orchestrator_lambda)
        api_caller_lambda.grant_invoke(orchestrator_lambda)

        #API CALLS LOG DATABASE PERMISSIONS
        #Allow functions to record when the make API calls
        api_calls_table.grant_write_data(aghanim_matches_lambda)
        api_calls_table.grant_write_data(api_caller_lambda)

        #Allow orchestrator to check the API calls table
        api_calls_table.grant_read_data(orchestrator_lambda)
        
        #OTHER HELPER DATABASE PERMISSIONS
        #Give appropriate ddb read/write access to functions
        # players_table.grant_read_write_data(update_players_lambda)
        # players_table.grant_read_data(find_matchids_lambda)
        # matchid_table.grant_read_write_data(find_matchids_lambda)
        aghanim_matches_table.grant_read_write_data(aghanim_matches_lambda)
        aghanim_matches_table.grant_read_data(api_caller_lambda)
        aghanim_query_window_table.grant_read_data(orchestrator_lambda)
        aghanim_query_window_table.grant_read_write_data(aghanim_matches_lambda)
        aghanim_query_window_table.grant_read_write_data(api_caller_lambda)
        aghanim_query_window_table.grant_read_data(queue_populator_lambda)
        