import os
import json
from query_handler import QueryHandler
from api_caller.lib import APICaller, API_Call_Metadata

class AghsMatchesHandler(QueryHandler):
    def __init__(self, ddb_resource, query_type, event, staging_queue) -> None:
        super().__init__(ddb_resource, query_type, event)

        #Load additional tables
        self.query_window_table = ddb_resource.Table(os.environ['AGHANIM_QUERY_WINDOW_TABLE'])
        self.old_errors = 0
        self.get_existing_log()
        self.reached_end = False
        
        self.staging_queue = staging_queue

        self.output_table = ddb_resource.Table(os.environ['AGHANIM_MATCHES_TABLE'])

    def get_existing_log(self):
        item = self.query_window_table.get_item(Key={'start_time': self.event['start_time'], 'difficulty': self.event['difficulty']})
        if 'Item' in item:
            self.old_errors = item['Item']['errors']

    def extract_vars(self):
        self.variables = {
            'createdAfterDateTime': self.event['start_time'],
            'createdBeforeDateTime': self.event['start_time'] + self.event['window'],
            'difficulty': self.event['difficulty'],
            'take': self.event['take'],
            'skip': self.event['skip']
        }
        return self.variables

    def make_query(self):
        super().make_query()

    def write_results(self):
        self.matches = self.query_result['data']['stratz']['page']['aghanim']['matches']
        if len(self.matches) < self.variables['take']:
            self.reached_end = True
        print(f'Found {len(self.matches)} matches')


        with self.output_table.batch_writer() as batch:
            for match in self.matches:
                batch.put_item(Item=match)
        return self.matches

    def get_errors(self):
        return len(self.query_result.get('errors', []))

    def log_window(self):
        print(f'Processing window {self.event["window"]}')
        item = {
            'start_time': self.event['start_time'],
            'window': self.event['window'],
            'difficulty': self.event['difficulty'],
            'errors': self.old_errors + self.get_errors(),
            'processed': len(self.matches) + self.variables['skip'],
            'reached_end': self.reached_end
        }
        self.query_window_table.put_item(Item=item)

    def queue_next_query(self):
        if self.reached_end:
            return
       
        aghs_payload = {
            "query_type": self.query_type,
            "window": self.event['window'],
            "start_time": self.event['start_time'],
            "difficulty": self.event['difficulty'],
            "take": self.event['take'],
            "skip": self.event['take'] + self.event['skip']
        }

        print(f'Queueing next query: {json.dumps(aghs_payload)}')
        self.staging_queue.send_message(MessageBody=aghs_payload)