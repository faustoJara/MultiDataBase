# create_dbs.py

import mysql.connector
import psycopg2
from psycopg2 import sql
from pymongo import MongoClient

# --- CONFIGURACIÓN DE CONEXIÓN (AJUSTAR SEGÚN TU ENTORNO DOCKER) ---
DB_NAME = "CENTROCUIDADOFAMILIAR"
DB_CONFIG = {
    'mysql': {
        'user': 'root',
        'password': 'mi_clave', 
        'host': '127.0.0.1',
        'port': 3306
    },
    'postgres': {
        'user': 'postgres',
        'password': 'mi_clave',
        'host': '127.0.0.1',
        'port': 5433
    },
    'mongo_uri': "mongodb://127.0.0.1:27017/"
}

# --- DEFINICIÓN DE ESTRUCTURAS SQL (Común para ambas, adaptando sintaxis) ---

# MySQL utiliza AUTO_INCREMENT y DATETIME
MYSQL_TABLES_SQL = """
CREATE DATABASE IF NOT EXISTS CENTROCUIDADOFAMILIAR;
USE CENTROCUIDADOFAMILIAR;

CREATE TABLE IF NOT EXISTS USUARIA (
    IDUsuario INT PRIMARY KEY AUTO_INCREMENT,
    Nombre VARCHAR(100) NOT NULL,
    DNI VARCHAR(20) UNIQUE NOT NULL,
    RentaPercapita DECIMAL(10, 2),
    Email VARCHAR(100) UNIQUE
);

CREATE TABLE IF NOT EXISTS CUIDADOR (
    IDCuidador INT PRIMARY KEY AUTO_INCREMENT,
    Nombre VARCHAR(100) NOT NULL,
    Especialidad VARCHAR(100)
);

CREATE TABLE IF NOT EXISTS SERVICIO (
    IDServicio INT PRIMARY KEY AUTO_INCREMENT,
    IDUsuario INT NOT NULL,
    IDCuidador INT,
    FechaHoraInicio DATETIME NOT NULL,
    PrecioFinal DECIMAL(10, 2) NOT NULL,
    FOREIGN KEY (IDUsuario) REFERENCES USUARIA(IDUsuario),
    FOREIGN KEY (IDCuidador) REFERENCES CUIDADOR(IDCuidador)
);
"""

# PostgreSQL utiliza SERIAL y TIMESTAMP
POSTGRES_TABLES_SQL = """
-- No se puede usar CREATE DATABASE en una transacción, se hace por separado.
CREATE TABLE IF NOT EXISTS USUARIA (
    IDUsuario SERIAL PRIMARY KEY,
    Nombre VARCHAR(100) NOT NULL,
    DNI VARCHAR(20) UNIQUE NOT NULL,
    RentaPercapita NUMERIC(10, 2),
    Email VARCHAR(100) UNIQUE
);

CREATE TABLE IF NOT EXISTS CUIDADOR (
    IDCuidador SERIAL PRIMARY KEY,
    Nombre VARCHAR(100) NOT NULL,
    Especialidad VARCHAR(100)
);

CREATE TABLE IF NOT EXISTS SERVICIO (
    IDServicio SERIAL PRIMARY KEY,
    IDUsuario INT NOT NULL REFERENCES USUARIA(IDUsuario),
    IDCuidador INT REFERENCES CUIDADOR(IDCuidador),
    FechaHoraInicio TIMESTAMP NOT NULL,
    PrecioFinal NUMERIC(10, 2) NOT NULL
);
"""

# --- FUNCIONES DE CREACIÓN ---

def create_mysql_structure():
    print("-> Creando estructura en MySQL...")
    conn = None
    try:
        # 1. Conexión y Creación de DB
        conn = mysql.connector.connect(**DB_CONFIG['mysql'])
        cursor = conn.cursor()
        
        # Ejecutar todas las sentencias (incluyendo CREATE DATABASE y USE)
        for statement in MYSQL_TABLES_SQL.split(';'):
            if statement.strip():
                cursor.execute(statement)
        
        conn.commit()
        print("  MySQL: Estructura creada con éxito.")

    except mysql.connector.Error as err:
        print(f"  MySQL ERROR: {err.msg}")
    finally:
        if conn and conn.is_connected():
            cursor.close()
            conn.close()

def create_postgres_structure():
    print("-> Creando estructura en PostgreSQL...")
    temp_conn = None
    conn = None
    try:
        # 1. Conexión a DB 'postgres' para asegurar que CENTROCUIDADOFAMILIAR exista
        temp_conn = psycopg2.connect(**DB_CONFIG['postgres'], database="postgres")
        temp_conn.autocommit = True
        cursor = temp_conn.cursor()
        
        # Crear la base de datos si no existe
        try:
            cursor.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(DB_NAME)))
        except psycopg2.errors.DuplicateDatabase:
            pass # Ya existe, no hay problema
        
        cursor.close()
        temp_conn.close()

        # 2. Conexión a la DB recién creada y creación de tablas
        conn = psycopg2.connect(**DB_CONFIG['postgres'], database=DB_NAME)
        cursor = conn.cursor()
        
        for statement in POSTGRES_TABLES_SQL.split(';'):
            if statement.strip():
                cursor.execute(statement)
        
        conn.commit()
        print("  PostgreSQL: Estructura creada con éxito.")

    except psycopg2.Error as err:
        print(f" PostgreSQL ERROR: {err}")
    finally:
        if conn:
            conn.close()

def create_mongodb_structure():
    print("-> Creando colecciones en MongoDB...")
    try:
        client = MongoClient(DB_CONFIG['mongo_uri'])
        db = client[DB_NAME]
        
        # MongoDB crea colecciones al insertar, pero podemos forzar la existencia e índices
        collections = ['usuarios_mongo', 'cuidadores_mongo', 'servicios_mongo', 'logs']
        
        for col_name in collections:
            db.create_collection(col_name) # La crea si no existe
            
        db['servicios_mongo'].create_index([("IDUsuario", 1)])
        
        client.close()
        print("  MongoDB: Colecciones creadas con éxito.")
        
    except Exception as e:
        print(f" MongoDB ERROR: {e}")

if __name__ == '__main__':
    print("--- INICIO DE CREACIÓN DE ESTRUCTURAS DE BASES DE DATOS ---")
    create_mysql_structure()
    create_postgres_structure()
    create_mongodb_structure()
   