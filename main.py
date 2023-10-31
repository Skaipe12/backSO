from fastapi import FastAPI
from fastapi.responses import HTMLResponse
from pydantic import BaseModel
import boto3
import psycopg2
from psycopg2 import sql
from fastapi.responses import JSONResponse
from dotenv import load_dotenv
import os

app = FastAPI()

load_dotenv('keys.env')

app.tittle = "API S.O"

# Configuración de la base de datos RDS
rds_config = {
    'dbname': os.getenv('DATABASE_NAME'),
    'user': os.getenv('DATABASE_USER'),
    'password': os.getenv('DATABASE_PASSWORD'),
    'host': os.getenv('DATABASE_HOST'),
    'port': os.getenv('DATABASE_PORT')
}

# Configuración de S3
s3_config = {
    'aws_access_key_id': os.getenv('AWS_ACCESS_KEY_ID'),
    'aws_secret_access_key': os.getenv('AWS_SECRET_ACCESS_KEY'),
    'bucket_name': os.getenv('S3_BUCKET_NAME')
}

class Pelicula(BaseModel):
    nombre: str
    rating: float
    description: str

@app.get("/", tags=["Root"])
def message():
    return HTMLResponse('<h1>Hello World<h1>')

@app.post("/peliculas", tags=["Pelicula"])
def cargar_pelicula(pelicula: Pelicula):
    try:
        # Conexión a RDS
        connection = psycopg2.connect(**rds_config)
        cursor = connection.cursor()
        cursor.execute(
            sql.SQL("INSERT INTO brayaneduardocine (nombre, rating, description) VALUES (%s, %s, %s)"),
            (pelicula.nombre, pelicula.rating, pelicula.description)
        )
        connection.commit()  

        # Conexión a S3
        s3 = boto3.client('s3', aws_access_key_id=s3_config['aws_access_key_id'], aws_secret_access_key=s3_config['aws_secret_access_key'])

        
        with open('peliculas.txt', 'w') as file:
            file.write(f'Nombre: {pelicula.nombre}, Rating: {pelicula.rating}, Descripción: {pelicula.description}')

        numero_archivos = len(s3.list_objects(Bucket=s3_config['bucket_name'])['Contents'])
        nuevo_nombre_archivo = f'archivo_{numero_archivos + 1}.txt'

        s3.upload_file('peliculas.txt', s3_config['bucket_name'], nuevo_nombre_archivo)


        cursor.close()
        connection.close()
    
        return JSONResponse(status_code=201, content={"message": "Pelicula creada correctamente"})
    except Exception as e:
        return JSONResponse(status_code=500, content={"message": str(e)})

#Endpoint que filtre los datos de la tabla de rds por nombre
@app.get("/peliculas/{nombre}", tags=["Pelicula"])
def obtener_pelicula(nombre: str):
    # Conexión a RDS
    connection = psycopg2.connect(**rds_config)
    cursor = connection.cursor()
    
    cursor.execute(sql.SQL("SELECT * FROM BrayanEduardoCine WHERE nombre = {}").format(
        sql.Literal(nombre)
    ))
    pelicula = cursor.fetchone()
    cursor.close()
    connection.close()
    return pelicula

#Endpoint que cuente la cantidad de archivos en un bucket de s3
@app.get("/peliculas", tags=["Pelicula"])
def obtener_archivos_s3():
    # Conexión a S3
    s3 = boto3.client('s3', aws_access_key_id=s3_config['aws_access_key_id'], aws_secret_access_key=s3_config['aws_secret_access_key'])
    cantidad = len(s3.list_objects(Bucket=s3_config['bucket_name'])['Contents'])
    return cantidad

