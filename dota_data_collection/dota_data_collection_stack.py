from aws_cdk import core as cdk

# For consistency with other languages, `cdk` is the preferred import name for
# the CDK's core module.  The following line also imports it as `core` for use
# with examples from the CDK Developer's Guide, which are in the process of
# being updated to use `cdk`.  You may delete this import if you don't need it.
from aws_cdk import (
    aws_dynamodb as ddb,
    aws_lambda_python as py_lambda,
    aws_lambda as _lambda,
    aws_sqs as sqs,
    aws_events as events,
    aws_secretsmanager as sm,
    aws_iam as iam,
    core
)
from aws_cdk.aws_lambda_python import PythonFunction

class DotaDataCollectionStack(cdk.Stack):

    def __init__(self, scope: cdk.Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # Handle Stratz API Key
        stratz_apikey = sm.Secret.from_secret_name_v2(self, "StratzAPIKey", secret_name="stratz/apikey")
        stratz_api_caller = iam.Role(self, 'StratzAPICaller',
            assumed_by=iam.ServicePrincipal('lambda.amazonaws.com'))
        

        # Create a DynamoDB table
        players_table = ddb.Table(self, "PlayersTable",
                                    partition_key=ddb.Attribute(name="player_id", type=ddb.AttributeType.NUMBER),
                                    billing_mode=ddb.BillingMode.PAY_PER_REQUEST
                                )
        match_table = ddb.Table(self, "MatchTable",
                                    partition_key=ddb.Attribute(name="match_id", type=ddb.AttributeType.NUMBER),
                                )
        api_calls_table = ddb.Table(self, "APICallsTable",
                                    partition_key=ddb.Attribute(name="api_call_id", type=ddb.AttributeType.NUMBER),
                                    billing_mode=ddb.BillingMode.PAY_PER_REQUEST
                                )

        # Lambda Functions
        LAMBDA_ENVS = {
                        "TABLE_NAME": players_table.table_name,
                        "API_CALLS_TABLE": api_calls_table.table_name,
                        "PARTITION_KEY": "player_id",
        }
        update_players_lambda = PythonFunction(self, "UpdatePlayersLambda",
                                                    runtime=_lambda.Runtime.PYTHON_3_8,
                                                    entry='lambda/update_players',
                                                    index='update_players.py',
                                                    handler='handler',
                                                    environment=LAMBDA_ENVS,
                                                    timeout=core.Duration.minutes(10),
                                                    profiling=True,
                                                )
        
        queue_players_lambda = PythonFunction(self, "QueuePlayersLambda",
                                                    runtime=_lambda.Runtime.PYTHON_3_8,
                                                    entry='lambda/queue_players',
                                                    index='queue_players.py',
                                                    handler='handler',
                                                    environment=LAMBDA_ENVS,
                                                    timeout=core.Duration.minutes(10),
                                                    profiling=True,
                                                )
        LAMBDA_FUNC_NAMES = {
            "UPDATE_PLAYERS_FUNC_NAME": update_players_lambda.function_name,
        }
        orchestrator_lambda = PythonFunction(self, "OrchestratorLambda",
                                                    runtime=_lambda.Runtime.PYTHON_3_8,
                                                    entry='lambda/orchestrator',
                                                    index='orchestrator.py',
                                                    handler='handler',
                                                    environment={**LAMBDA_ENVS, **LAMBDA_FUNC_NAMES},
                                                    timeout=core.Duration.minutes(10),
                                                    profiling=True,
                                                )              


        #STRATZ API KEY ACCESS
        #Give Stratz API Key access to functions that call the API
        stratz_apikey.grant_read(update_players_lambda)
        stratz_apikey.grant_read(queue_players_lambda)

        #ALLOW FUNCTION CALLS
        #Allow the orchestrator function to call other functions
        update_players_lambda.grant_invoke(orchestrator_lambda)
        queue_players_lambda.grant_invoke(orchestrator_lambda)

        #API CALLS LOG DATABASE PERMISSIONS
        #Allow functions to record when the make API calls
        api_calls_table.grant_write_data(update_players_lambda)
        api_calls_table.grant_write_data(queue_players_lambda)

        #Allow orchestrator to check the API calls table
        api_calls_table.grant_read_data(orchestrator_lambda)
        
        #OTHER HELPER DATABASE PERMISSIONS
        #Give appropriate ddb read/write access to functions
        players_table.grant_read_write_data(update_players_lambda)
