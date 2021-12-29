import os
import json
from query_handler import QueryHandler
from api_caller.lib import APICaller, API_Call_Metadata
import pandas as pd
import numpy as np

class AghsMatchesHandler(QueryHandler):
    def __init__(self, ddb_resource, query_type, event, staging_queue, conn, cur) -> None:
        super().__init__(ddb_resource, query_type, event)

        #Load additional tables
        self.query_window_table = ddb_resource.Table(os.environ['AGHANIM_QUERY_WINDOW_TABLE'])
        self.old_errors = 0
        self.get_existing_log()
        self.reached_end = False
        
        self.staging_queue = staging_queue

        self.conn = conn
        self.cur = cur

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

        if len(self.matches) > 0:
            self.write_to_csv()

            self.cur.copy_from(open('/tmp/matches.csv'), 'matches', sep=',')
            self.cur.copy_from(open('/tmp/players.csv'), 'players', sep=',')
            self.cur.copy_from(open('/tmp/player_depthlist.csv'), 'playerDepthList', sep=',')
            self.cur.copy_from(open('/tmp/player_blessings.csv'), 'playerBlessings', sep=',')
            self.cur.copy_from(open('/tmp/depthlist.csv'), 'depthList', sep=',')
            self.cur.copy_from(open('/tmp/ascensionabilities.csv'), 'ascenionAbilities', sep=',')
            self.conn.commit()

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
    


    def write_to_csv(self):
        #Handle matches
        def to_csv_wrapper(df, fpath):
            df.to_csv(fpath, index=False, float_format = '%.0f', header=False, na_rep='NULL')

        df = pd.DataFrame(self.matches)
        df = df.drop(['players', 'depthList'], axis=1)
        df['startDateTime'] = pd.to_datetime(df['startDateTime'], unit='s')
        df['endDateTime'] = pd.to_datetime(df['endDateTime'], unit='s')
        #Make bool lowercase if needed
        to_csv_wrapper(df, '/tmp/matches.csv')
        del df

        player_dfs = []
        for match in self.matches:
            player_dfs.append(pd.DataFrame(match['players']))
        player_df = pd.concat(player_dfs) 
        player_df = player_df.drop(['depthList', 'blessings'], axis=1)

        for i in range(6):
            col = f'item{i}Id'
            player_df[col] = player_df[col].replace(['None', 'nan'], np.nan)
            
        player_df['neutral0Id'] = player_df['neutral0Id'].replace(['None', 'nan'], np.nan)
        player_df['neutralItemId'] = player_df['neutralItemId'].replace(['None', 'nan'], np.nan)

        to_csv_wrapper(player_df, '/tmp/players.csv')
        del player_df

        player_depthlist_rows = []
        for match in self.matches:
            row = {}
            row['matchId'] = match['id']
            for player in match['players']:
                row['playerSlot'] = player['playerSlot']
                row['depth'] = 0
                row['steamAccountId'] = player['steamAccountId']
                if player['depthList']:
                    for depth_item in player['depthList']:
                        ser = pd.concat([pd.Series(row), pd.Series(depth_item)])
                        player_depthlist_rows.append(ser)
                        row['depth'] += 1

        player_depthlist_df = pd.concat(player_depthlist_rows, axis=1).T
        to_csv_wrapper(player_depthlist_df, '/tmp/player_depthlist.csv')
        del player_depthlist_df

        player_blessings_rows = []
        for match in self.matches:
            row = {}
            row['matchId'] = match['id']
            for player in match['players']:
                row['playerSlot'] = player['playerSlot']
                row['steamAccountId'] = player['steamAccountId']
                for blessing in player['blessings']:
                    ser = pd.concat([pd.Series(row), pd.Series(blessing)])
                    player_blessings_rows.append(ser)

        player_blessings_df = pd.concat(player_blessings_rows, axis=1).T
        to_csv_wrapper(player_blessings_df, '/tmp/player_blessings.csv')
        del player_blessings_df

        depthlist_rows = []
        for match in self.matches:
            row = {}
            row['matchId'] = match['id']
            row['depth'] = 0
            if match['depthList']:
                for depth in match['depthList']:
                    ser = pd.concat([pd.Series(row), pd.Series(depth)])
                    depthlist_rows.append(ser)
                    row['depth'] += 1

        depthlist_df = pd.concat(depthlist_rows, axis=1).T
        depthlist_df = depthlist_df.drop(['ascensionAbilities'], axis=1)
        to_csv_wrapper(depthlist_df, '/tmp/depthlist.csv')
        del depthlist_df

        ascensionabilities_rows = []
        for match in self.matches:
            row = {}
            row['matchId'] = match['id']
            row['depth'] = 0
            if match['depthList']:
                for depth in match['depthList']:
                    if depth['ascensionAbilities']:
                        for ascensionability in depth['ascensionAbilities']:
                            ser = pd.concat([pd.Series(row), pd.Series(ascensionability)])
                            ascensionabilities_rows.append(ser)
                    row['depth'] += 1

        ascensionabilities_df = pd.concat(ascensionabilities_rows, axis=1).T
        to_csv_wrapper(ascensionabilities_df, '/tmp/ascensionabilities.csv')
        del ascensionabilities_df