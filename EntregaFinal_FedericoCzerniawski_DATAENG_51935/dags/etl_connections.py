import psycopg2 as psy

config_dic = {}

def redshift_conectar():
    #Crear string de conexión y generar el engine
    try:
        conn = psy.connect(host=config_dic['HOST_REDSHIFT'], dbname=config_dic['DATABASE_REDSHIFT'], user=config_dic['USER_REDSHIFT'], password=config_dic['PASS_REDSHIFT'], port=config_dic['PORT_REDSHIFT'])
        conn_cur = conn.cursor()
        print("LOG: >>> Conexión a la BD OK.")
        conn_cur.close()

        return conn
    
    except:
        print("LOG: >>> Error al conectar a la base de datos.")
        return None

def redshift_crear_factica(**kwargs):
    #Crea la tabla fáctica en RedShift
    
    config_dic.update(kwargs['dag'].default_args)

    table_name = config_dic['FACT_TABLE_NAME']

    sql_drop_table = f"""DROP TABLE IF EXISTS {table_name};"""

    sql_create_table = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            id INT PRIMARY KEY,
            player_id INT,
            player_fst_name VARCHAR(100),
            player_lst_name VARCHAR(100),
            player_is_active BOOL,
            team_id INT,
            team VARCHAR(256),
            team_abb VARCHAR(10),
            team_city VARCHAR(100),
            team_state VARCHAR(100),
            team_year_founded INT,
            description VARCHAR(256),
            all_nba_team_number INT,
            season VARCHAR(10) distkey,
            award_type VARCHAR(25),
            award_subtype1 VARCHAR(25),
            award_subtype2 VARCHAR(25),
            fh_proceso timestamp
        ) SORTKEY(player_id);
    """
    
    sql_count = f"""SELECT count(*) FROM {table_name};"""

    conn = redshift_conectar()

    try:
        with conn.cursor() as cur:
            if config_dic['REGENERAR_FACTICA'] == 1:
                cur.execute(sql_drop_table)

            cur.execute(sql_create_table)
            cur.execute(sql_count)
            n_rows = cur.fetchone()
        
        conn.commit()
        conn.close()
    
        print(f"LOG: Cantidad de registros en la fáctica: {n_rows[0]}")
    except:
        print("LOG: \t\tError al generar la fáctica.")

def redshift_eliminar_registros(**kwargs):
    config_dic.update(kwargs['dag'].default_args)
    
    table_name = config_dic['FACT_TABLE_NAME']
    column_name = 'PLAYER_ID'
    value = config_dic['PLAYER_ID_UPD']

    sql_delete_values = f"DELETE FROM {table_name} WHERE {column_name} = {value};"
    sql_count = f"""SELECT count(*) FROM {table_name};"""
    
    print(f"LOG: \t>>> Eliminando registros de {table_name}")
    
    try:
        conn = redshift_conectar()
        with conn.cursor() as cur:
            cur.execute(sql_delete_values)
            cur.execute(sql_count)
            n_rows = cur.fetchone()
    
        conn.commit()
        conn.close()

        print(f"LOG: Cantidad de registros en la fáctica: {n_rows[0]}")
    except:
        print("LOG: \t\tError al eliminar los registros.")