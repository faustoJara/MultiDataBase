import mysql.connector
import psycopg2
from psycopg2 import sql
from pymongo import MongoClient


DB_NAME = "centrocuidadofamiliar"   # Nombre de la base de datos en minúsculas para PostgreSQL y MongoDB CENTROCUIDADOFAMILIAR
DB_CONFIG = {
    'mysql': {
        'user': 'root',
        'password': '0853',         # AJUSTAR mi_clave a 0853
        'host': '127.0.0.1',  #  '127.0.0.1'
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




MYSQL_TABLES_SQL = f"""
CREATE DATABASE IF NOT EXISTS {DB_NAME};
USE {DB_NAME};

CREATE TABLE IF NOT EXISTS USUARIA (
    IDUsuario INT PRIMARY KEY AUTO_INCREMENT,
    Nombre VARCHAR(100) NOT NULL,
    Apellido VARCHAR(100) NOT NULL,
    DNI VARCHAR(20) UNIQUE NOT NULL,
    FechaNacimiento DATE,
    Barrio VARCHAR(100),
    RentaPercapita DECIMAL(10, 2), 
    Telefono VARCHAR(20),
    Email VARCHAR(100) UNIQUE,
    Genero VARCHAR(50) 
);

CREATE TABLE IF NOT EXISTS DEPENDIENTE (
    IDDependiente INT PRIMARY KEY AUTO_INCREMENT,
    IDUsuario INT NOT NULL,
    Nombre VARCHAR(100) NOT NULL,
    Apellido VARCHAR(100) NOT NULL,
    TipoDependencia VARCHAR(50),
    FechaNacimiento DATE,
    PerfilMedico TEXT, 
    FOREIGN KEY (IDUsuario) REFERENCES USUARIA(IDUsuario)
);

CREATE TABLE IF NOT EXISTS CUIDADOR (
    IDCuidador INT PRIMARY KEY AUTO_INCREMENT,
    Nombre VARCHAR(100) NOT NULL,
    Apellido VARCHAR(100) NOT NULL,
    DNI VARCHAR(20) UNIQUE NOT NULL,
    Telefono VARCHAR(20),
    Disponibilidad VARCHAR(255),
    Especialidad VARCHAR(100)
);

CREATE TABLE IF NOT EXISTS CENTRO (
    IDCentro INT PRIMARY KEY AUTO_INCREMENT,
    NombreCentro VARCHAR(100) NOT NULL,
    Direccion VARCHAR(255),
    DescripcionCentro TEXT,
    CapacidadMaxima INT
);

CREATE TABLE IF NOT EXISTS SERVICIO (
    IDServicio INT PRIMARY KEY AUTO_INCREMENT,
    IDUsuario INT NOT NULL,
    IDDependiente INT NOT NULL,
    IDCuidador INT,
    IDCentro INT,
    FechaHoraInicio DATETIME NOT NULL, 
    FechaHoraFin DATETIME NOT NULL,
    PrecioBase DECIMAL(10, 2) NOT NULL,
    SubvencionAplicada DECIMAL(10, 2) DEFAULT 0.00,
    PrecioFinal DECIMAL(10, 2) NOT NULL,
    Estado VARCHAR(50),
    
    FOREIGN KEY (IDUsuario) REFERENCES USUARIA(IDUsuario),
    FOREIGN KEY (IDDependiente) REFERENCES DEPENDIENTE(IDDependiente),
    FOREIGN KEY (IDCuidador) REFERENCES CUIDADOR(IDCuidador),
    FOREIGN KEY (IDCentro) REFERENCES CENTRO(IDCentro)
);

CREATE TABLE IF NOT EXISTS TRANSACCION (
    IDTransaccion INT PRIMARY KEY AUTO_INCREMENT,
    IDServicio INT UNIQUE NOT NULL,
    FechaTransaccion DATETIME DEFAULT CURRENT_TIMESTAMP, 
    Monto DECIMAL(10, 2) NOT NULL,
    EstadoPago VARCHAR(50),
    
    FOREIGN KEY (IDServicio) REFERENCES SERVICIO(IDServicio)
);

CREATE TABLE IF NOT EXISTS RESENA (
    IDResena INT PRIMARY KEY AUTO_INCREMENT,
    IDServicio INT UNIQUE NOT NULL,
    IDUsuario INT NOT NULL,
    Puntuacion INT,
    Comentario TEXT,
    FechaResena DATETIME DEFAULT CURRENT_TIMESTAMP,
    
    FOREIGN KEY (IDServicio) REFERENCES SERVICIO(IDServicio),
    FOREIGN KEY (IDUsuario) REFERENCES USUARIA(IDUsuario)
);
""" 

# PostgreSQL utiliza SERIAL y TIMESTAMP WITHOUT TIME ZONE/NUMERIC
POSTGRES_TABLES_SQL = f"""
CREATE TABLE IF NOT EXISTS USUARIA (
    IDUsuario SERIAL PRIMARY KEY,
    Nombre VARCHAR(100) NOT NULL,
    Apellido VARCHAR(100) NOT NULL,
    DNI VARCHAR(20) UNIQUE NOT NULL,
    FechaNacimiento DATE,
    Barrio VARCHAR(100),
    RentaPercapita NUMERIC(10, 2), 
    Telefono VARCHAR(20),
    Email VARCHAR(100) UNIQUE,
    Genero VARCHAR(50) CHECK (Genero IN ('Femenino', 'Masculino', 'Otro'))
);

CREATE TABLE IF NOT EXISTS DEPENDIENTE (
    IDDependiente SERIAL PRIMARY KEY,
    IDUsuario INT NOT NULL REFERENCES USUARIA(IDUsuario),
    Nombre VARCHAR(100) NOT NULL,
    Apellido VARCHAR(100) NOT NULL,
    TipoDependencia VARCHAR(50) CHECK (TipoDependencia IN ('Niño', 'Mayor', 'Otro')),
    FechaNacimiento DATE,
    PerfilMedico TEXT
);

CREATE TABLE IF NOT EXISTS CUIDADOR (
    IDCuidador SERIAL PRIMARY KEY,
    Nombre VARCHAR(100) NOT NULL,
    Apellido VARCHAR(100) NOT NULL,
    DNI VARCHAR(20) UNIQUE NOT NULL,
    Telefono VARCHAR(20),
    Disponibilidad VARCHAR(255),
    Especialidad VARCHAR(100)
);

CREATE TABLE IF NOT EXISTS CENTRO (
    IDCentro SERIAL PRIMARY KEY,
    NombreCentro VARCHAR(100) NOT NULL,
    Direccion VARCHAR(255),
    DescripcionCentro TEXT,
    CapacidadMaxima INT
);

CREATE TABLE IF NOT EXISTS SERVICIO (
    IDServicio SERIAL PRIMARY KEY,
    IDUsuario INT NOT NULL REFERENCES USUARIA(IDUsuario),
    IDDependiente INT NOT NULL REFERENCES DEPENDIENTE(IDDependiente),
    IDCuidador INT REFERENCES CUIDADOR(IDCuidador),
    IDCentro INT REFERENCES CENTRO(IDCentro),
    FechaHoraInicio TIMESTAMP WITHOUT TIME ZONE NOT NULL, 
    FechaHoraFin TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    PrecioBase NUMERIC(10, 2) NOT NULL,
    SubvencionAplicada NUMERIC(10, 2) DEFAULT 0.00,
    PrecioFinal NUMERIC(10, 2) NOT NULL,
    Estado VARCHAR(50) CHECK (Estado IN ('Pendiente', 'Asignado', 'En Curso', 'Completado', 'Cancelado'))
);

CREATE TABLE IF NOT EXISTS TRANSACCION (
    IDTransaccion SERIAL PRIMARY KEY,
    IDServicio INT UNIQUE NOT NULL REFERENCES SERVICIO(IDServicio),
    FechaTransaccion TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    Monto NUMERIC(10, 2) NOT NULL,
    EstadoPago VARCHAR(50) CHECK (EstadoPago IN ('Pendiente', 'Completado', 'Fallido', 'Reembolsado'))
);

CREATE TABLE IF NOT EXISTS RESENA (
    IDResena SERIAL PRIMARY KEY,
    IDServicio INT UNIQUE NOT NULL REFERENCES SERVICIO(IDServicio),
    IDUsuario INT NOT NULL REFERENCES USUARIA(IDUsuario),
    Puntuacion INT CHECK (Puntuacion >= 1 AND Puntuacion <= 5),
    Comentario TEXT,
    FechaResena TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);
"""

# --- FUNCIONES DE CREACIÓN (Mismas que tenías) ---

def create_mysql_structure():
    print("-> Creando estructura en MySQL...")
    conn = None
    try:
        conn = mysql.connector.connect(**DB_CONFIG['mysql'])
        cursor = conn.cursor()
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
        temp_conn = psycopg2.connect(**DB_CONFIG['postgres'], database="postgres")
        temp_conn.autocommit = True
        cursor = temp_conn.cursor()
        try:
            cursor.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(DB_NAME)))
        except psycopg2.errors.DuplicateDatabase:
            pass
        cursor.close()
        temp_conn.close()

        conn = psycopg2.connect(**DB_CONFIG['postgres'], database=DB_NAME)
        cursor = conn.cursor()
        for statement in POSTGRES_TABLES_SQL.split(';'):
            if statement.strip():
                cursor.execute(statement)
        conn.commit()
        print("  PostgreSQL: Estructura creada con éxito.")

    except psycopg2.Error as err:
        # Importante: para el error FATAL: password authentication failed
        if "FATAL" in str(err) or "refused" in str(err):
            print(f" PostgreSQL ERROR: ¡Error de conexión/credenciales! Revisa tu clave y puerto (5433).")
        else:
            print(f" PostgreSQL ERROR: {err}")
    finally:
        if conn:
            conn.close()

def create_mongodb_structure():
    print("-> Creando colecciones en MongoDB...")
    try:
        client = MongoClient(DB_CONFIG['mongo_uri'])
        db = client[DB_NAME]
        
        collections = ['usuarios_mongo', 'dependientes_mongo', 'cuidadores_mongo', 'centros_mongo', 'servicios_mongo', 'transacciones_mongo', 'resenas_mongo', 'logs']
        
        for col_name in collections:
            db.create_collection(col_name)
            
        db['servicios_mongo'].create_index([("IDUsuario", 1), ("IDCuidador", 1)])
        db['transacciones_mongo'].create_index([("IDServicio", 1)], unique=True)
        
        client.close()
        print("  MongoDB: Colecciones creadas con éxito.")
        
    except Exception as e:
        print(f" MongoDB ERROR: {e}")

if __name__ == '__main__':
    print("--- INICIO DE CREACIÓN DE ESTRUCTURAS DE BASES DE DATOS ---")
    create_mysql_structure()
    create_postgres_structure()
    create_mongodb_structure()