#https://rapidapi.com/theapiguy/api/free-nba/details
#pip install nba_api

#https://aws.amazon.com/blogs/big-data/use-the-amazon-redshift-sqlalchemy-dialect-to-interact-with-amazon-redshift/
#pip install sqlalchemy-redshift
#pip install redshift_connector

from nba_api.stats.static import players, teams
from nba_api.stats.endpoints import playerawards
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.engine.url import URL
import time
#from sqlalchemy import table,column,insert
#from sqlalchemy.sql import text
#from sqlalchemy.orm import scoped_session, sessionmaker

players_lst = players.get_players()
teams_lst = teams.get_teams()
#playercareer_dict = playercareerstats.PlayerCareerStats(player_id=1629638).get_dict()

#Obtengo los jugadores
print(len(players_lst))
print(len(teams_lst))
#print('Awards header')
#print(playerawards_dict['resultSets'][0]['headers'])
#print('Awards result')
#print(playerawards_dict['resultSets'][0]['rowSet'])

#Conectar a la BD
db_engine = create_engine('postgresql://postgres:postgres@localhost:5435/nba')

#Conectar a Redshift
rs_url = URL.create(
    drivername='redshift+redshift_connector',
    host='data-engineer-cluster.cyhh5bfevlmn.us-east-1.redshift.amazonaws.com',
    port=5439,
    database='data-engineer-database',
    username='fedec_88_coderhouse',
    password='74K8fnY4z2'
)
redshift_engine = create_engine(rs_url)

#Cargar los datos de los equipos
df = pd.DataFrame(teams_lst)
df.to_sql('equipos', db_engine, index=False, if_exists='replace')
df.to_sql('equipos', redshift_engine, index=False, if_exists='replace')

#Cargar los datos de jugadores
df = pd.DataFrame(players_lst)
df.to_sql('jugadores', db_engine, index=False, if_exists='replace')

#Cargar los datos de premiacion por jugador
i = 0
for player in players_lst:
    print(player['id'])
    
    #Obtengo los premios del jugador
    player_awards = playerawards.PlayerAwards(player_id=player['id'], timeout=60)
    df = pd.DataFrame(player_awards.get_data_frames()[0])

    #Si el data frame no está vacío, lo cargo en la BD
    if not df.empty:
        df.to_sql('premios_x_jugador', db_engine, index=True, index_label='id', if_exists='append')
        i +=1 
    #Espero 0.1 segundos para ejecutar el próximo request
    #time.sleep(0.1)

print(f'Jugadores con Premios: {i}')