#Cifrado y carga de archivo de config
import rsa, json, base64, os

#Variables globales
config_dic = {
    'FILES_PATH': os.path.join('archivos')
}

# def pruebita(**kwargs):
#     #print(f">>>>>>>>>>>>> FEDE_TEST: {config_dic['FACT_TABLE_NAME']}")
#     print(f">>>>>>>>>>>>> FEDE_TEST: {kwargs['dag'].default_args}")
#     print(f">>>>>>>>>>>>> FEDE_TEST_2: {os.environ['REDSHIFT_USER']}")

def file_path(file):
    return os.path.abspath(os.path.join(config_dic['FILES_PATH'], file))

def cargar_configuracion():
    #Completa el diccionario global con la configuración necesaria
    #fullpath = os.path.join(os.getcwd(), config_dic['CONFIG_PATH'])
    #os.chdir(fullpath)
    print(f'Directorio actual: {os.getcwd()}')

    #Cargar private key
    with open(file_path('private.pem'), 'r') as file:
        privKey = rsa.PrivateKey.load_pkcs1(file.read())

    #Cargar archivo de configuración
    with open(file_path('config.json'), 'r') as file:
        jConfig = json.load(file)

    #Recupero variables de entorno
    config_dic['URL_REDSHIFT'] = os.getenv('REDSHIFT_URL') #f"jdbc:postgresql://{config_dic['HOST_REDSHIFT']}:{config_dic['PORT_REDSHIFT']}/{config_dic['DATABASE_REDSHIFT']}?user={config_dic['USER_REDSHIFT']}&password={config_dic['PASS_REDSHIFT']}"
    config_dic['DATABASE_REDSHIFT'] = os.getenv('REDSHIFT_DB') #rsa.decrypt(base64.b64decode(jConfig['cDATABASE_REDSHIFT']), privKey).decode()
    config_dic['SCHEMA_REDSHIFT'] = os.getenv('REDSHIFT_SCHEMA') #config_dic['USER_REDSHIFT']
    config_dic['HOST_REDSHIFT'] = os.getenv('REDSHIFT_HOST') #jConfig['HOST_REDSHIFT']
    config_dic['PORT_REDSHIFT'] = os.getenv('REDSHIFT_PORT') #rsa.decrypt(base64.b64decode(jConfig['cPORT_REDSHIFT']), privKey).decode()
    config_dic['JDBC_DRIVER_REDSHIFT'] = os.getenv('JDBC_DRIVER_REDSHIFT') #jConfig['JDBC_DRIVER_REDSHIFT']

    #Recupero parametros del JSON
    config_dic['USER_REDSHIFT'] = rsa.decrypt(base64.b64decode(jConfig['cUSER_REDSHIFT']), privKey).decode()
    config_dic['PASS_REDSHIFT'] = rsa.decrypt(base64.b64decode(jConfig['cPASS_REDSHIFT']), privKey).decode()
    config_dic['FACT_TABLE_NAME'] = jConfig['FACT_TABLE_NAME']
    config_dic['REQUEST_TIMEOUT'] = jConfig['REQUEST_TIMEOUT']
    config_dic['PLAYER_LIMIT'] = jConfig['PLAYER_LIMIT']
    config_dic['ERROR_LIMIT'] = jConfig['ERROR_LIMIT']
    config_dic['REGENERAR_FACTICA'] = jConfig['REGENERAR_FACTICA']
    config_dic['PLAYER_ID_UPD'] = jConfig['PLAYER_ID_UPD']
    config_dic['CSV_PREMIOS'] = file_path(jConfig['CSV_PREMIOS'])
    config_dic['SCRIPTS_PATH'] = os.path.abspath('scripts')
    config_dic['DRIVER_PATH'] = file_path(os.getenv('DRIVER_JAR'))
    
    #Setear variables de entorno
    #os.environ["SPARK_CLASSPATH"] = config_dic['DRIVER_PATH']
    #os.environ["PYSPARK_SUBMIT_ARGS"] = f"--driver-class-path {config_dic['DRIVER_PATH']} --jars {config_dic['DRIVER_PATH']} pyspark-shell"

    return config_dic