#Manejo de API
#https://rapidapi.com/theapiguy/api/free-nba/details
#pip install nba_api
from nba_api.stats.static import players, teams
from nba_api.stats.endpoints import playerawards

#Manipulación de AWS
#https://aws.amazon.com/blogs/big-data/use-the-amazon-redshift-sqlalchemy-dialect-to-interact-with-amazon-redshift/
#pip install sqlalchemy-redshift
#pip install redshift_connector
import sqlalchemy as sa
from sqlalchemy import orm as sa_orm
from sqlalchemy.engine.url import URL

#Cifrado y carga de archivo de config
import rsa, json, base64, os
from os import path

#Variables y constantes globales
CONFIG_PATH = 'Archivos'

config_dic = { #Diccionario con la configuración de la app
    'request_timout': 50,   #Timeout para los requests
    'player_limit': 50,      #Límite de jugadores a procesar. -1: Sin limite.
    'error_limit': 100       #Límite de errores a partir del cual se aborta la carga.
} 

def cargar_configuracion():
    #Completa el diccionario global con la configuración necesaria

    #Setear directorio
    fullpath = path.join(os.getcwd(), CONFIG_PATH)
    os.chdir(fullpath)
    print(f'Directorio actual: {os.getcwd()}')

    #Cargar private key
    with open('private.pem', 'r') as file:
        privKey = rsa.PrivateKey.load_pkcs1(file.read())

    #Cargar archivo de configuración
    with open('config.json', 'r') as file:
        jConfig = json.load(file)

    config_dic['cHost'] = jConfig['host']
    config_dic['cPort'] = rsa.decrypt(base64.b64decode(jConfig['cPort']), privKey).decode()
    config_dic['cDatabase'] = rsa.decrypt(base64.b64decode(jConfig['cDatabase']), privKey).decode()
    config_dic['cUser'] = rsa.decrypt(base64.b64decode(jConfig['cUser']), privKey).decode()
    config_dic['cPass'] = rsa.decrypt(base64.b64decode(jConfig['cPass']), privKey).decode()

def redshift_conectar():
    #Crear string de conexión y generar el engine

    rs_url = URL.create(
        drivername='redshift+redshift_connector',
        host= config_dic['cHost'],
        port= config_dic['cPort'],
        database= config_dic['cDatabase'],
        username= config_dic['cUser'],
        password= config_dic['cPass']
    )
    return sa.create_engine(rs_url)

def redshift_crear_factica(table_name, engine):
    #Crea la tabla fáctica en RedShift
    metadata = sa.MetaData()

    factTable = sa.Table(
        table_name, metadata,
        sa.Column('id', sa.BIGINT),                     #fact table index
        sa.Column('player_id', sa.BIGINT),              #player.id
        sa.Column('first_name', sa.VARCHAR(100)),       #player.first_name
        sa.Column('last_name', sa.VARCHAR(100)),        #player.last_name
        sa.Column('is_active', sa.BOOLEAN),             #player.is_active
        sa.Column('team_id', sa.BIGINT),                #teams.id
        sa.Column('team', sa.VARCHAR(256)),             #teams.full_name
        sa.Column('team_abb', sa.VARCHAR(10)),          #teams.abbreviation
        sa.Column('team_city', sa.VARCHAR(100)),        #teams.city
        sa.Column('team_state', sa.VARCHAR(100)),       #teams.state
        sa.Column('team_year_founded', sa.INT),         #teams.year_founded
        sa.Column('description', sa.VARCHAR(256)),      #playerawards.description
        sa.Column('all_nba_team_number', sa.INT),       #playerawards.all_nba_team_number
        sa.Column('season', sa.VARCHAR(10)),            #playerawards.season
        sa.Column('award_type', sa.VARCHAR(25)),        #playerawards.type
        sa.Column('award_subtype1', sa.VARCHAR(25)),    #playerawards.subtype1
        sa.Column('award_subtype2', sa.VARCHAR(25)),    #playerawards.subtype2
        redshift_diststyle='KEY',
        redshift_distkey='season',
        redshift_sortkey='player_id')

    #Si existe la tabla, la elimino
    if sa.inspect(engine).has_table(table_name):
        factTable.drop(bind=engine)
    # Crear la tabla
    factTable.create(bind=engine)

    return factTable

if __name__ == '__main__':
    #Cargar la configuración desde el archivo
    cargar_configuracion()

    #Conectar a Redshift y crear la sesion
    redshift_engine = redshift_conectar()
    
    Session = sa_orm.sessionmaker()
    Session.configure(bind=redshift_engine)

    #Crear la tabla fáctica de los premios por jugador por temporada por equipo
    table_name = 'f_premios_obtenidos'

    rsPremiosFactTable = redshift_crear_factica(table_name, redshift_engine)


    ##  *************** EXTRACT *************** 
    print(f">>>> Comienza el proceso de extracción")

    #Recuperar los equipos
    teams_lst = teams.get_teams()
    print(f"Cantidad de equipos: {len(teams_lst)}")

    #Recuperar los jugadores
    players_lst = players.get_players()
    print(f"Cantidad de jugadores: {len(players_lst)}")


    ##  *************** LOAD *************** 
    print(f">>>> Comienza la carga de la fáctica {table_name}")

    def checkInt(value):
    #Se utiliza para verificar que el valor obtenido sea INT, caso contrario retorna 0.
        try:
            return int(value)
        except:
            return 0
        
    #Cargar los datos de premiacion por Jugador | Equipo | Temporada (fáctica)
    nPlayers = 0    #Cantidad de jugadores procesados
    idxPremio = 0   #Indice de la fáctica
    errorCount = 0  #Cantidad de errores en el proceso

    for player in players_lst:
        if config_dic['player_limit'] != -1 and nPlayers >= config_dic['player_limit']:
            print(f"Se alcanzó el limite de jugadores procesados. Limite seteado: {config_dic['player_limit']}")
            break

        if errorCount >= config_dic['error_limit']:
            print(f"Se alcanzó el limite de errores permitidos. Limite seteado: {config_dic['error_limit']}")
            break

        #print(f"Procesando jugador: {player['id']}")
        nPlayers += 1
        if nPlayers % 10 == 0:
            print(f"Van procesados {nPlayers} jugadores...")

        #Obtener los premios del jugador
        try:
            premios = playerawards.PlayerAwards(player_id= player['id'], timeout=config_dic['request_timout']).get_normalized_dict()['PlayerAwards']
        except:
            print(f"Error al recuperar premios del jugador {player['id']}")
            errorCount += 1
            continue
        
        with Session() as session:
            for premio in premios:
                #Recupero datos del equipo
                try:
                    team = [team for team in teams_lst if team['full_name'].lower() == premio['TEAM'].lower()][0]
                except:
                    #En caso de que no se informe el equipo, se agrega un genérico en la fáctica.
                    team = {'id': 0, 'full_name':'-', 'abbreviation':'-', 'city': '-', 'state': '-', 'year_founded': 0}
                    errorCount += 1
                
                try:
                    idxPremio += 1  #Aumenta el indice de la fáctica

                    insert_data_row = rsPremiosFactTable.insert().values(
                        id = idxPremio,
                        player_id = player['id'],
                        first_name = player['first_name'],
                        last_name = player['last_name'],
                        is_active = player['is_active'],
                        team_id = team['id'],
                        team = team['full_name'],
                        team_abb = team['abbreviation'],
                        team_city = team['city'],
                        team_state = team['state'],
                        team_year_founded = checkInt(team['year_founded']),
                        description = premio['DESCRIPTION'],
                        all_nba_team_number = checkInt(premio['ALL_NBA_TEAM_NUMBER']),
                        season = premio['SEASON'],
                        award_type = premio['TYPE'],
                        award_subtype1 = premio['SUBTYPE1'],
                        award_subtype2 = premio['SUBTYPE2']
                    )
                    
                    session.execute(insert_data_row)
                    session.commit()
                    
                except Exception as e:
                    print(f"Error al insertar registro de id {idxPremio}. Msj: {e}")
                    errorCount += 1
                    pass

    #Obtengo la cantidad de filas en la fáctica
    with Session() as session:
        nFactRows = session.query(rsPremiosFactTable).count()

    print(f"Jugadores procesados: {nPlayers}")
    print(f"Premios insertados en la fáctica: {nFactRows}")
    print(f"Cantidad de errores durante el proceso: {errorCount}")