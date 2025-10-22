import mysql.connector
import psycopg2
import psycopg2.extras
from pymongo import MongoClient
import json
from datetime import datetime
from decimal import Decimal

# --- CONFIGURACIÓN DE CONEXIÓN  ---
DB_NAME = "CENTROCUIDADOFAMILIAR"
RENT_THRESHOLD = 30000.00
DB_CONFIG = {
    'mysql': {
        'user': 'root',
        'password': 'mi_clave',
        'host': '127.0.0.1',
        'port': 3306,
        'database': DB_NAME
    },
    'postgres': {
        'user': 'postgres',
        'password': 'mi_clave',
        'host': '127.0.0.1',
        'port': 5433,
        'database': DB_NAME
    },
    'mongo_uri': "mongodb://127.0.0.1:27017/"
}

OUTPUT_FILE = "usuarias_renta_alta.json"

# Función para convertir tipos de datos que JSON no entiende
def default_converter(obj):
    if isinstance(obj, datetime):
        return obj.isoformat()
    if isinstance(obj, (Decimal, float)):
        return float(obj)
    if isinstance(obj, set):
        return list(obj)
    raise TypeError(f"Objeto de tipo {type(obj)} no es serializable por JSON")

# ----------------------------------------------------------------------
# 1. Extracción de MySQL
# ----------------------------------------------------------------------
def get_mysql_data():
    print("-> Extrayendo usuarias con renta > 30,000 de MySQL...")
    conn = None
    data = []
    
    QUERY = f"""
    SELECT
        IDUsuario, Nombre, Apellido, Genero, Barrio, RentaPercapita, Email
    FROM
        USUARIA
    WHERE
        RentaPercapita > {RENT_THRESHOLD}
    """
    try:
        conn = mysql.connector.connect(**DB_CONFIG['mysql'])
        cursor = conn.cursor(dictionary=True)
        cursor.execute(QUERY)
        data = cursor.fetchall()
        print(f" MySQL: Encontradas {len(data)} usuarias.")
        return data

    except mysql.connector.Error as err:
        print(f" MySQL ERROR: {err.msg}")
        return []
        
    finally:
        if conn and conn.is_connected():
            conn.close()

# ----------------------------------------------------------------------
# 2. Extracción de PostgreSQL
# ----------------------------------------------------------------------
def get_postgres_data():
    print("-> Extrayendo usuarias con renta > 30,000 de PostgreSQL...")
    conn = None
    data = []
    
    # En PostgreSQL se usa el operador de concatenación de strings para el f-string
    QUERY = f"""
    SELECT
        IDUsuario, Nombre, Apellido, Genero, Barrio, RentaPercapita, Email
    FROM
        USUARIA
    WHERE
        RentaPercapita > {RENT_THRESHOLD}
    """
    try:
        conn = psycopg2.connect(**DB_CONFIG['postgres'])
        cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        cursor.execute(QUERY)
        # Convertir a lista de diccionarios estándar
        data = [dict(row) for row in cursor.fetchall()]
        print(f"  PostgreSQL: Encontradas {len(data)} usuarias.")
        return data

    except psycopg2.Error as err:
        print(f" PostgreSQL ERROR: {err}")
        return []
        
    finally:
        if conn:
            conn.close()

# ----------------------------------------------------------------------
# 3. Extracción de MongoDB
# ----------------------------------------------------------------------
def get_mongodb_data():
    print("-> Extrayendo usuarias con renta > 30,000 de MongoDB...")
    data = []
    try:
        client = MongoClient(DB_CONFIG['mongo_uri'])
        db = client[DB_NAME]
        
        # Consulta de MongoDB: filtrar por RentaPercapita > 30000
        cursor = db['usuarios_mongo'].find({
            "DatosSensibles.RentaPercapita": {"$gt": RENT_THRESHOLD}
        }, 
        # Proyección (campos a devolver, similar al SELECT)
        {"IDUsuario": 1, "NombreCompleto": 1, "DatosSensibles.Genero": 1, 
         "DatosSensibles.Barrio": 1, "DatosSensibles.RentaPercapita": 1, 
         "Contacto.email": 1, "_id": 0})
        
        for doc in cursor:
            # Reestructurar el documento para que coincida con el formato SQL
            data.append({
                'IDUsuario': doc.get('IDUsuario'),
                'NombreCompleto': doc.get('NombreCompleto'),
                'Genero': doc.get('DatosSensibles', {}).get('Genero'),
                'Barrio': doc.get('DatosSensibles', {}).get('Barrio'),
                'RentaPercapita': doc.get('DatosSensibles', {}).get('RentaPercapita'),
                'Email': doc.get('Contacto', {}).get('email'),
                'Fuente': 'MongoDB' # Añadir la fuente para el JSON final
            })
            
        print(f" MongoDB: Encontradas {len(data)} usuarias.")
        client.close()
        return data
        
    except Exception as e:
        print(f" MongoDB ERROR: {e}")
        return []

# ----------------------------------------------------------------------
# 4. Consolidación y Generación de JSON
# ----------------------------------------------------------------------
def consolidate_and_generate_json():
    
    # 1. Obtener datos de cada fuente
    mysql_data = get_mysql_data()
    postgres_data = get_postgres_data()
    mongo_data = get_mongodb_data()
    
    # 2. Etiquetar y consolidar
    for row in mysql_data:
        row['Fuente'] = 'MySQL'
    for row in postgres_data:
        row['Fuente'] = 'PostgreSQL'
    # Mongo data ya fue etiquetada en la función get_mongodb_data
    
    consolidated_data = mysql_data + postgres_data + mongo_data
    
    # 3. Formato Final
    final_json = {
        "metadata": {
            "fecha_consolidacion": datetime.now().isoformat(),
            "criterio_filtro": f"Renta Percapita Superior a {RENT_THRESHOLD} €",
            "total_registros_consolidados": len(consolidated_data)
        },
        "usuarias_renta_alta": consolidated_data
    }

    # 4. Guardar en archivo JSON
    try:
        with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
            # Usar 'default' para manejar tipos como Decimal (MySQL/Postgres)
            json.dump(final_json, f, default=default_converter, ensure_ascii=False, indent=4)
        print(f"\n¡Éxito! Archivo JSON consolidado generado: {OUTPUT_FILE}")
    except Exception as e:
        print(f"ERROR al guardar el JSON: {e}")

if __name__ == '__main__':
    consolidate_and_generate_json()