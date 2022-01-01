from aws_cdk import core as cdk

from aws_cdk import (
    aws_dynamodb as ddb,
    aws_lambda as _lambda,
    aws_sqs as sqs,
    aws_events as events,
    aws_events_targets as targets,
    aws_secretsmanager as sm,
    aws_ec2 as ec2,
    aws_rds as rds,
    aws_iam as iam,
    core
)

from aws_cdk.aws_lambda_event_sources import SqsEventSource

class DotaDataCollectionStack(cdk.Stack):

    def __init__(self, scope: cdk.Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # Stratz API Key
        stratz_apikey = sm.Secret.from_secret_name_v2(self, "StratzAPIKey", secret_name="stratz/apikey")

        # SQS Queues
        staging_queue = sqs.Queue(self, 'StagingQueue',
            visibility_timeout=core.Duration.seconds(300),
        )
        api_caller_queue = sqs.Queue(self, 'APICallerQueue',
            visibility_timeout=core.Duration.seconds(180),
        )
        failure_queue = sqs.Queue(self, 'FailureQueue',
            visibility_timeout=core.Duration.seconds(300),
        )

        # DynamoDB tables
        def create_ddb(table_name: str, partition_key: ddb.Attribute, sort_key: ddb.Attribute = None) -> ddb.Table:
            return ddb.Table(
                self, table_name,
                partition_key=partition_key,
                sort_key=sort_key,
                billing_mode=ddb.BillingMode.PAY_PER_REQUEST,
                removal_policy=core.RemovalPolicy.DESTROY
            )

        # Helper data structures
        players_table = create_ddb("PlayersTable",
            partition_key=ddb.Attribute(name="player_id", type=ddb.AttributeType.NUMBER),
        )
        matchid_table = create_ddb("MatchIDTable",
            partition_key=ddb.Attribute(name="match_id", type=ddb.AttributeType.NUMBER),
        )
        api_calls_table = create_ddb("APICallsTable",
            partition_key=ddb.Attribute(name="api_call_id", type=ddb.AttributeType.NUMBER),
        )        
        aghanim_query_window_table = create_ddb("AghanimQueryWindowTable",
            partition_key=ddb.Attribute(name="start_time", type=ddb.AttributeType.NUMBER),
            sort_key=ddb.Attribute(name="difficulty", type=ddb.AttributeType.STRING),
        )

        # Results tables
        aghanim_matches_table = create_ddb("AghanimMatchesTable",
            partition_key=ddb.Attribute(name="id", type=ddb.AttributeType.NUMBER),
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
            "AGHANIM_QUERY_WINDOW_TABLE": aghanim_query_window_table.table_name,

            "STAGING_QUEUE": staging_queue.queue_name,
            "API_CALLER_QUEUE": api_caller_queue.queue_name,
            "FAILURE_QUEUE": failure_queue.queue_name,

            "AGHANIM_MATCHES_TABLE": aghanim_matches_table.table_name,
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
            timeout=core.Duration.minutes(3),
            memory_size=256,
            profiling=True,
            layers=[api_caller_layer],
        )

        LAMBDA_FUNC_NAMES = {
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
            timeout=core.Duration.seconds(50),
            profiling=True,
        )

        queue_populator_lambda = _lambda.Function(
            self, "QueuePopulatorLambda",
            runtime=_lambda.Runtime.PYTHON_3_8,
            handler='index.handler',
            code=_lambda.Code.from_asset(
                "lambda/queue_populator",
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

        failure_repopulator_lambda = _lambda.Function(
            self, "FailureRepopulatorLambda",
            runtime=_lambda.Runtime.PYTHON_3_8,
            handler='index.handler',
            code=_lambda.Code.from_asset(
                "lambda/failure_repopulator",
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

        # LAMBDA EVENTS
        # Attach api caller lambda to SQS queue
        api_caller_lambda.add_event_source(SqsEventSource(api_caller_queue,
            batch_size=1,
        ))
        # Run orchestrator every minute
        minute_rule = events.Rule(self, "MinuteRule",
            schedule=events.Schedule.cron(minute='*/1')
        )
        minute_rule.add_target(targets.LambdaFunction(orchestrator_lambda))


        #STRATZ API KEY ACCESS
        #Give Stratz API Key access to functions that call the API
        stratz_apikey.grant_read(api_caller_lambda)

        #QUEUE PERMISSIONS
        #Give the queues access to the functions that call the API
        staging_queue.grant_consume_messages(orchestrator_lambda)
        staging_queue.grant_send_messages(orchestrator_lambda)
        api_caller_queue.grant_send_messages(orchestrator_lambda)
        api_caller_queue.grant_consume_messages(api_caller_lambda)
        staging_queue.grant_send_messages(api_caller_lambda)
        staging_queue.grant_send_messages(queue_populator_lambda)
        failure_queue.grant_send_messages(api_caller_lambda)
        failure_queue.grant_consume_messages(failure_repopulator_lambda)
        api_caller_queue.grant_send_messages(failure_repopulator_lambda)


        #ALLOW FUNCTION CALLS
        #Allow the orchestrator function to call other functions
        api_caller_lambda.grant_invoke(orchestrator_lambda)

        #API CALLS LOG DATABASE PERMISSIONS
        #Allow functions to record when the make API calls
        api_calls_table.grant_write_data(api_caller_lambda)

        #Allow orchestrator to check the API calls table
        api_calls_table.grant_read_data(orchestrator_lambda)
        
        #OTHER HELPER DATABASE PERMISSIONS
        #Give appropriate ddb read/write access to functions
        # players_table.grant_read_write_data(update_players_lambda)
        # players_table.grant_read_data(find_matchids_lambda)
        # matchid_table.grant_read_write_data(find_matchids_lambda)
        aghanim_query_window_table.grant_read_data(orchestrator_lambda)
        aghanim_query_window_table.grant_read_write_data(api_caller_lambda)
        aghanim_query_window_table.grant_read_data(queue_populator_lambda)
        
        #Results tables
        aghanim_matches_table.grant_read_write_data(api_caller_lambda)