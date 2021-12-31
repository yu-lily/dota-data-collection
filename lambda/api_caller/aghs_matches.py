import os
import json
from query_handler import QueryHandler
from api_caller.lib import APICaller, API_Call_Metadata
import pandas as pd
import numpy as np

class AghsMatchesHandler(QueryHandler):
    def __init__(self, ddb_resource, query_type, event, sqs_resource) -> None:
        super().__init__(ddb_resource, query_type, event)

        #Load additional tables
        self.query_window_table = ddb_resource.Table(os.environ['AGHANIM_QUERY_WINDOW_TABLE'])
        self.old_errors = 0
        self.get_existing_log()
        self.reached_end = False
        
        self.staging_queue = sqs_resource.get_queue_by_name(QueueName=os.environ["STAGING_QUEUE"])

        self.aghanim_matches = ddb_resource.Table(os.environ['AGHANIM_MATCHES_TABLE'])
        self.aghanim_players = ddb_resource.Table(os.environ['AGHANIM_PLAYERS_TABLE'])
        self.aghanim_player_depthlist = ddb_resource.Table(os.environ['AGHANIM_PLAYER_DEPTHLIST_TABLE'])
        self.aghanim_player_blessings = ddb_resource.Table(os.environ['AGHANIM_PLAYER_BLESSINGS_TABLE'])
        self.aghanim_depthlist = ddb_resource.Table(os.environ['AGHANIM_DEPTHLIST_TABLE'])
        self.aghanim_ascensionabilities = ddb_resource.Table(os.environ['AGHANIM_ASCENSIONABILITIES_TABLE'])


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

        match_items = []
        player_items = []
        player_depthlist_items = []
        player_blessings_items = []
        depthlist_items = []
        ascensionabilities_items = []

        #Unpack match data
        for match in self.matches:
            match_id = match['id']
            if match['players']:
                for player in match.get('players', []):
                    player_slot = player['playerSlot']
                    depth = 0
                    if player['depthList']:
                        for player_depth in player.get('depthList', []):
                            player_depth['matchId-playerSlot'] = f'{match_id}{player_slot}'
                            player_depth['depth'] = depth
                            depth += 1
                            player_depthlist_items.append(player_depth)
                        player.pop('depthList')

                    if player['blessings']:
                        for player_blessing in player.get('blessings', []):
                            player_blessing['matchId-playerSlot'] = f'{match_id}{player_slot}'
                            player_blessings_items.append(player_blessing)
                        player.pop('blessings')
                    
                    player_items.append(player)
                match.pop('players')

            depth = 0
            if match['depthList']:
                for depth_entry in match.get('depthList', []):
                    if depth_entry['ascensionAbilities']:
                        for ascensionability in depth_entry.get('ascensionAbilities', []):
                            ascensionability['matchId-depth'] = f'{match_id}{depth}'
                            ascensionabilities_items.append(ascensionability)
                    depth_entry.pop('ascensionAbilities')

                    depth_entry['matchId'] = match_id
                    depth_entry['depth'] = depth
                    depthlist_items.append(depth_entry)
                    depth += 1
                match.pop('depthList')

            match_items.append(match)

        #Insert match data
        def batch_write(table, items):
            with table.batch_writer() as batch:
                for item in items:
                    batch.put_item(Item=item)

        batch_write(self.aghanim_matches, match_items)
        batch_write(self.aghanim_players, player_items)
        batch_write(self.aghanim_player_depthlist, player_depthlist_items)
        batch_write(self.aghanim_player_blessings, player_blessings_items)
        batch_write(self.aghanim_depthlist, depthlist_items)
        batch_write(self.aghanim_ascensionabilities, ascensionabilities_items)

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
        self.staging_queue.send_message(MessageBody=json.dumps(aghs_payload))
    
    def insert_matches(self):
        print('Inserting matches')
        for match in self.matches:
            match_id = match['id']
            match_data = match['data']
            self.insert_match(match_id, match_data)