#!pip install nba_api

from nba_api.stats.static import players, teams
from nba_api.stats.endpoints import playerawards

import pandas as pa

config_dic = {}

def etl_extract_test(**kwargs):
    config_dic.update(kwargs['dag'].default_args)

    df = pa.DataFrame(teams.get_teams())
    #df = pa.read_csv(config_dic['CSV_PREMIOS'])

    print(df)