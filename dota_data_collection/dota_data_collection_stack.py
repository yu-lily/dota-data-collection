from aws_cdk import core as cdk

# For consistency with other languages, `cdk` is the preferred import name for
# the CDK's core module.  The following line also imports it as `core` for use
# with examples from the CDK Developer's Guide, which are in the process of
# being updated to use `cdk`.  You may delete this import if you don't need it.
from aws_cdk import (
    aws_dynamodb as ddb,
    aws_lambda_python as py_lambda,
    aws_sqs as sqs,
    aws_events as events,
    aws_secretsmanager as sm,
    core
)
from aws_cdk.aws_lambda_python import PythonFunction

class DotaDataCollectionStack(cdk.Stack):

    def __init__(self, scope: cdk.Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # Create secrets manager
        secret_name = "dota_api_key"
        secret = sm.Secret(self, "Secret",
                            secret_name=secret_name,

        # Create a DynamoDB table
        players_table = ddb.Table(self, "PlayersTable",
                                    partition_key=ddb.Attribute(name="player_id", type=ddb.AttributeType.NUMBER),
                                )
                                    #billing_mode=ddb.BillingMode.PAY_PER_REQUEST)
        match_table = ddb.Table(self, "MatchTable",
                                    partition_key=ddb.Attribute(name="match_id", type=ddb.AttributeType.NUMBER),
                                )
        
        # Create a Lambda function
        update_players_lambda = PythonFunction(self, "UpdatePlayersLambda",
                                                    runtime=py_lambda.Runtime.PYTHON_3_7,
                                                    entry='lambda/update_players',
                                                    index='update_players.py',
                                                    handler='handler',
                                                    environment={
                                                        "TABLE_NAME": players_table.table_name,
                                                        "PARTITION_KEY": "player_id",
                                                        "SORT_KEY": "player_name",
                                                        "DDB_REGION": "us-east-1",
                                                        "DDB_ENDPOINT": "http://localhost:8000"
                                                    }
                                                )
        queue_players_lambda = PythonFunction(self, "QueuePlayersLambda",
                                                    runtime=py_lambda.Runtime.PYTHON_3_7,
                                                    entry='lambda/queue_players',
                                                    index='queue_players.py',
                                                    handler='handler',
                                                    environment={
                                                        "TABLE_NAME": players_table.table_name,
                                                        "PARTITION_KEY": "player_id",
                                                        "SORT_KEY": "player_name",
                                                        "DDB_REGION": "us-east-1",
                                                        "DDB_ENDPOINT": "http://localhost:8000"
                                                    }
                                                )
        