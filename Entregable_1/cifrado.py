import rsa
import base64
import json
import os
from os import path

#Crear el directorio
fullpath = path.join(os.getcwd(),'Coderhouse\Archivos')

if not os.path.exists(fullpath):
    os.mkdir(fullpath)

os.chdir(fullpath)
print(f'Directorio actual: {os.getcwd()}')

pHost = 'data-engineer-cluster.cyhh5bfevlmn.us-east-1.redshift.amazonaws.com'
pPort = '5439'
pDatabase = 'data-engineer-database'
pUser = 'fedec_88_coderhouse'
pPass = '74K8fnY4z2'

publicKey, privateKey = rsa.newkeys(512)

#Generar clave publica
key = publicKey.save_pkcs1()
with open('public.pem', 'w') as file:
    file.write(key.decode())

#Generar clave privada
key = privateKey.save_pkcs1()
with open('private.pem', 'w') as file:
    file.write(key.decode())

#Cifrar parametros
encPort = base64.b64encode(rsa.encrypt(pPort.encode(),publicKey)).decode()
encDatabase = base64.b64encode(rsa.encrypt(pDatabase.encode(),publicKey)).decode()
encUser = base64.b64encode(rsa.encrypt(pUser.encode(),publicKey)).decode()
encPass = base64.b64encode(rsa.encrypt(pPass.encode(),publicKey)).decode()

#Decifrado test
#encHost = rsa.decrypt(encHost, privateKey).decode()
#print(f'Host decifrado: {encHost}')

#Crear json de configuracion
dictConfig = {
    'host': pHost,
    'cPort': encPort,
    'cDatabase': encDatabase,
    'cUser': encUser,
    'cPass': encPass
}

with open('config.json','w') as outfile:
    json.dump(dictConfig, outfile)