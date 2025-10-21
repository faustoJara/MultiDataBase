import random
import mysql.connector
import psycopg2
from pymongo import MongoClient
from faker import Faker

# --- CONFIGURACIÓN Y FAKER ---
DB_NAME = "CENTROCUIDADOFAMILIAR"
DB_CONFIG = {
    # Usamos la misma configuración que en el script de creación
    'mysql': { 'user': 'root', 'password': 'mi_clave', 'host': '127.0.0.1', 'port': 3306, 'database': DB_NAME },
    'postgres': { 'user': 'postgres', 'password': 'mi_clave', 'host': '127.0.0.1', 'port': 5433, 'database': DB_NAME },
    'mongo_uri': "mongodb://127.0.0.1:27017/"
}
NUM_RECORDS = 10
fake = Faker('es_ES')

# --- GENERACIÓN DE DATOS BASE ---

# Generamos 10 usuarios y 10 cuidadores (los datos son idénticos para ambas SQL)
USUARIOS_DATA = []
CUIDADORES_DATA = []
for i in range(1, NUM_RECORDS + 1):
    USUARIOS_DATA.append({
        'ID': i,
        'Nombre': fake.name(),
        'DNI': fake.ssn(),
        'Renta': round(random.uniform(10000, 40000), 2),
        'Email': fake.email()
    })
    CUIDADORES_DATA.append({
        'ID': i,
        'Nombre': fake.name(),
        'Especialidad': random.choice(['Infantil', 'Geriatría', 'Terapia'])
    })

# --- FUNCIONES DE POBLACIÓN SQL ---

def populate_sql(db_type, config, usuarios, cuidadores):
    print(f"-> Rellenando {db_type.upper()}...")
    conn = None
    try:
        conn = mysql.connector.connect(**config) if db_type == 'mysql' else psycopg2.connect(**config)
        cursor = conn.cursor()

        # 1. Insertar USUARIOS
        for user in usuarios:
            sql_user = f"INSERT INTO USUARIA (IDUsuario, Nombre, DNI, RentaPercapita, Email) VALUES (%s, %s, %s, %s, %s)"
            cursor.execute(sql_user, (user['ID'], user['Nombre'], user['DNI'], user['Renta'], user['Email']))

        # 2. Insertar CUIDADORES
        for cuidador in cuidadores:
            sql_cuidador = f"INSERT INTO CUIDADOR (IDCuidador, Nombre, Especialidad) VALUES (%s, %s, %s)"
            cursor.execute(sql_cuidador, (cuidador['ID'], cuidador['Nombre'], cuidador['Especialidad']))

        # 3. Insertar SERVICIOS
        for i in range(1, NUM_RECORDS + 1):
            user_id = random.randint(1, NUM_RECORDS)
            cuidador_id = random.choice([None, random.randint(1, NUM_RECORDS)])
            start_time = fake.date_time_this_year()
            precio = round(random.uniform(50.00, 150.00), 2)
            
            sql_servicio = f"INSERT INTO SERVICIO (IDUsuario, IDCuidador, FechaHoraInicio, PrecioFinal) VALUES (%s, %s, %s, %s)"
            cursor.execute(sql_servicio, (user_id, cuidador_id, start_time, precio))

        conn.commit()
        print(f"  {db_type.upper()}: {NUM_RECORDS} registros por tabla insertados.")

    except (mysql.connector.Error, psycopg2.Error) as err:
        print(f"  {db_type.upper()} ERROR: {err}")
    finally:
        if conn: conn.close()

def populate_mongodb(usuarios, cuidadores):
    print("-> Rellenando MongoDB...")
    try:
        client = MongoClient(DB_CONFIG['mongo_uri'])
        db = client[DB_NAME]
        
        # 1. Insertar USUARIOS (ejemplo de datos JSON enriquecidos)
        usuarios_mongo = [{
            'IDUsuario': u['ID'],
            'NombreCompleto': u['Nombre'],
            'DatosContacto': {'email': u['Email'], 'DNI': u['DNI']},
            'Renta': u['Renta']
        } for u in usuarios]
        db['usuarios_mongo'].insert_many(usuarios_mongo)

        # 2. Insertar CUIDADORES
        cuidadores_mongo = [{'IDCuidador': c['ID'], 'Nombre': c['Nombre'], 'Especialidad': c['Especialidad']} for c in cuidadores]
        db['cuidadores_mongo'].insert_many(cuidadores_mongo)
        
        # 3. Insertar SERVICIOS
        servicios_mongo = []
        for i in range(1, NUM_RECORDS + 1):
            servicios_mongo.append({
                'IDServicio': i,
                'IDUsuario': random.randint(1, NUM_RECORDS),
                'IDCuidador': random.choice([None, random.randint(1, NUM_RECORDS)]),
                'FechaHoraInicio': fake.date_time_this_year(),
                'PrecioFinal': round(random.uniform(50.00, 150.00), 2),
                'ComentarioInterno': fake.sentence()
            })
        db['servicios_mongo'].insert_many(servicios_mongo)

        client.close()
        print(f"  MongoDB: {NUM_RECORDS} documentos por colección insertados.")
        
    except Exception as e:
        print(f"  MongoDB ERROR: {e}")

if __name__ == '__main__':
    print("--- INICIO DE POBLACIÓN DE BASES DE DATOS CON FAKER ---")
    
    # 1. Poblar MySQL
    populate_sql('mysql', DB_CONFIG['mysql'], USUARIOS_DATA, CUIDADORES_DATA)
    
    # 2. Poblar PostgreSQL
    populate_sql('postgres', DB_CONFIG['postgres'], USUARIOS_DATA, CUIDADORES_DATA)
    
    # 3. Poblar MongoDB
    populate_mongodb(USUARIOS_DATA, CUIDADORES_DATA)
    
   