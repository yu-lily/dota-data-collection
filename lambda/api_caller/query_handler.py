from abc import ABC, abstractmethod
import os

from api_caller.lib import APICaller, API_Call_Metadata

class QueryHandler(ABC):
    def __init__(self, ddb_resource, query_type, event) -> None:
        self.query_type = query_type
        self.query = self.load_query()

        self.evnet = event

        self.variables = {}

        self.metadata = API_Call_Metadata(os.environ['AWS_LAMBDA_FUNCTION_NAME'],
            os.environ['AWS_LAMBDA_LOG_GROUP_NAME'],
            os.environ['AWS_LAMBDA_LOG_STREAM_NAME'],
            query_type)

        self.api_calls_table = ddb_resource.Table(os.environ['API_CALLS_TABLE'])
        self.api_caller = APICaller(self.api_calls_table)

    def load_query(self):
        with open(f'queries/{self.query_type}.txt', 'r') as f:
            return f.read()

    @abstractmethod
    def extract_vars(self, event):
        pass

    def make_query(self):
        self.query_result = self.api_caller.graphql_query(self.query, self.variables, self.metadata)

    @abstractmethod
    def write_results(self, data):
        pass
