#Cifrado y carga de archivo de config
import rsa, json, base64, os

#Variables globales
config_dic = {
    'FILES_PATH': os.path.join('archivos')
}

def file_path(file):
    return os.path.abspath(os.path.join(config_dic['FILES_PATH'], file))

def cargar_configuracion():
    #Completa el diccionario global con la configuración necesaria
    print(f'LOG: Directorio actual: {os.getcwd()}')

    #Cargar private key
    with open(file_path('private.pem'), 'r') as file:
        priv_key = rsa.PrivateKey.load_pkcs1(file.read())

    #Cargar archivo de configuración
    with open(file_path('config.json'), 'r') as file:
        j_config = json.load(file)

    #Recupero variables de entorno
    config_dic['DATABASE_REDSHIFT'] = os.getenv('REDSHIFT_DB')
    config_dic['SCHEMA_REDSHIFT'] = os.getenv('REDSHIFT_SCHEMA')
    config_dic['HOST_REDSHIFT'] = os.getenv('REDSHIFT_HOST')
    config_dic['PORT_REDSHIFT'] = os.getenv('REDSHIFT_PORT')
    config_dic['JDBC_DRIVER_REDSHIFT'] = os.getenv('JDBC_DRIVER_REDSHIFT')

    #Recupero parametros del JSON
    config_dic['USER_REDSHIFT'] = rsa.decrypt(base64.b64decode(j_config['cUSER_REDSHIFT']), priv_key).decode()
    config_dic['PASS_REDSHIFT'] = rsa.decrypt(base64.b64decode(j_config['cPASS_REDSHIFT']), priv_key).decode()
    config_dic['FACT_TABLE_NAME'] = j_config['FACT_TABLE_NAME']
    config_dic['REQUEST_TIMEOUT'] = j_config['REQUEST_TIMEOUT']
    config_dic['REGENERAR_FACTICA'] = j_config['REGENERAR_FACTICA']
    config_dic['PLAYER_ID_UPD'] = j_config['PLAYER_ID_UPD']
    config_dic['CSV_PREMIOS'] = file_path(j_config['CSV_PREMIOS'])
    config_dic['SCRIPTS_PATH'] = os.path.abspath('scripts')
    config_dic['DRIVER_PATH'] = file_path(os.getenv('DRIVER_JAR'))

    config_dic['URL_REDSHIFT'] = f"jdbc:postgresql://{config_dic['HOST_REDSHIFT']}:{config_dic['PORT_REDSHIFT']}/{config_dic['DATABASE_REDSHIFT']}?user={config_dic['USER_REDSHIFT']}&password={config_dic['PASS_REDSHIFT']}"

    return config_dic