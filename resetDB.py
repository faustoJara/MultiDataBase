import mysql.connector
import psycopg2
from psycopg2 import sql
from pymongo import MongoClient
import sys

# --- CONFIGURACIÓN DE CONEXIÓN (AJUSTAR) ---
DB_NAME = "CENTROCUIDADOFAMILIAR"
DB_CONFIG = {
    'mysql': {
        'user': 'root',
        'password': '0853',         # AJUSTAR mi_clave a 0853
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

def drop_mysql_database():
    print("-> Eliminando base de datos MySQL...")
    conn = None
    try:
        # Conexión sin especificar la DB para poder eliminarla
        config = DB_CONFIG['mysql'].copy()
        if 'database' in config:
            del config['database'] 
            
        conn = mysql.connector.connect(**config)
        cursor = conn.cursor()
        
        # Eliminar si existe
        cursor.execute(f"DROP DATABASE IF EXISTS {DB_NAME}")
        
        print(f" MySQL: Base de datos '{DB_NAME}' eliminada (o no existía).")

    except mysql.connector.Error as err:
        print(f" MySQL ERROR: {err.msg}")
        # Si el error es de conexión, detenemos el proceso
        if "Access denied" in err.msg or "refused" in err.msg:
            sys.exit(1)
    finally:
        if conn and conn.is_connected():
            cursor.close()
            conn.close()

def drop_postgres_database():
    print("-> Eliminando base de datos PostgreSQL...")
    conn = None
    try:
        # Conexión a la DB por defecto 'postgres' para poder eliminar la DB destino
        conn = psycopg2.connect(**DB_CONFIG['postgres'], database="postgres")
        conn.autocommit = True
        cursor = conn.cursor()
        
        # Primero, forzamos la desconexión de cualquier usuario que esté usando la DB
        cursor.execute(f"""
            SELECT pg_terminate_backend(pg_stat_activity.pid)
            FROM pg_stat_activity
            WHERE pg_stat_activity.datname = '{DB_NAME}'
            AND pid <> pg_backend_pid();
        """)
        
        # Eliminar si existe
        cursor.execute(sql.SQL("DROP DATABASE IF EXISTS {}").format(sql.Identifier(DB_NAME)))
        
        print(f" PostgreSQL: Base de datos '{DB_NAME}' eliminada (o no existía).")

    except psycopg2.Error as err:
        print(f" PostgreSQL ERROR: {err}")
    finally:
        if conn:
            conn.close()

def drop_mongodb_database():
    print("-> Eliminando base de datos MongoDB...")
    try:
        client = MongoClient(DB_CONFIG['mongo_uri'])
        client.drop_database(DB_NAME) 
        
        client.close()
        print(f" MongoDB: Base de datos '{DB_NAME}' eliminada.")
        
    except Exception as e:
        print(f" MongoDB ERROR: {e}")

if __name__ == '__main__':
    print("--- INICIO DE ELIMINACIÓN TOTAL DE BASES DE DATOS ---")
    drop_mysql_database()
    drop_postgres_database()
    drop_mongodb_database()
    print("--- PROCESO DE ELIMINACIÓN COMPLETADO ---")