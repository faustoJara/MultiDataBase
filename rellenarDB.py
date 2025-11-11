import random
import mysql.connector
import psycopg2
from pymongo import MongoClient
from faker import Faker
from datetime import timedelta

# --- CONFIGURACIÓN Y FAKER ---
DB_NAME = "centrocuidadofamiliar"   # Nombre de la base de datos en minúsculas para PostgreSQL y MongoDB CENTROCUIDADOFAMILIAR
DB_CONFIG = {
    'mysql': { 'user': 'root', 'password': '0853', 'host': '127.0.0.1', 'port': 3306, 'database': DB_NAME }, # AJUSTAR mi_clave a 0853 y el puerto 3306 a 3308
    'postgres': { 'user': 'postgres', 'password': 'mi_clave', 'host': '127.0.0.1', 'port': 5433, 'database': DB_NAME }, # AJUSTAR
    'mongo_uri': "mongodb://127.0.0.1:27017/"
}
NUM_BASE_RECORDS = 20 # Número base para usuarios, cuidadores, centros
NUM_SERVICIOS = 50 # Un mayor número de servicios para mejor análisis
fake = Faker('es_ES')

# --- GENERACIÓN DE DATOS BASE ---

USUARIOS_DATA = []
DEPENDIENTES_DATA = []
CUIDADORES_DATA = []
CENTROS_DATA = []

# ID Counters
u_counter, d_counter, c_counter, ce_counter, s_counter = 1, 1, 1, 1, 1

# Generar USUARIOS y DEPENDIENTES
for i in range(NUM_BASE_RECORDS):
    genero = random.choice(['Femenino', 'Masculino', 'Otro'])
    user_id = u_counter
    USUARIOS_DATA.append({
        'ID': user_id,
        'Nombre': fake.first_name(),
        'Apellido': fake.last_name(),
        'DNI': fake.ssn(),
        'FechaNacimiento': fake.date_of_birth(minimum_age=18, maximum_age=90),
        'Barrio': fake.city_suffix(),
        'Renta': round(random.uniform(10000, 60000), 2),
        'Telefono': fake.phone_number(),
        'Email': fake.email(),
        'Genero': genero
    })
    u_counter += 1

    # Crear al menos un DEPENDIENTE por cada USUARIO (para simplificar la inserción de servicios)
    dependiente_id = d_counter
    DEPENDIENTES_DATA.append({
        'ID': dependiente_id,
        'IDUsuario': user_id,
        'Nombre': fake.first_name(),
        'Apellido': fake.last_name(),
        'TipoDependencia': random.choice(['Niño', 'Mayor', 'Otro']),
        'FechaNacimiento': fake.date_of_birth(minimum_age=1, maximum_age=90),
        'PerfilMedico': fake.text(max_nb_chars=100)
    })
    d_counter += 1

# Generar CUIDADORES
for i in range(NUM_BASE_RECORDS):
    CUIDADORES_DATA.append({
        'ID': c_counter,
        'Nombre': fake.first_name(),
        'Apellido': fake.last_name(),
        'DNI': fake.ssn(),
        'Telefono': fake.phone_number(),
        'Disponibilidad': random.choice(['Mañana', 'Tarde', 'Noche', 'Completa']),
        'Especialidad': random.choice(['Infantil', 'Geriatría', 'Terapia Ocupacional', 'Movilidad'])
    })
    c_counter += 1

# Generar CENTROS
for i in range(5):
    CENTROS_DATA.append({
        'ID': ce_counter,
        'NombreCentro': fake.company(),
        'Direccion': fake.address(),
        'DescripcionCentro': fake.catch_phrase(),
        'CapacidadMaxima': random.randint(5, 50)
    })
    ce_counter += 1


# --- FUNCIONES DE POBLACIÓN SQL ---

def populate_sql(db_type, config):
    print(f"-> Rellenando {db_type.upper()}...")
    conn = None
    try:
        conn = mysql.connector.connect(**config) if db_type == 'mysql' else psycopg2.connect(**config)
        cursor = conn.cursor()

        # Limpiar tablas (para desarrollo)
        # Nota: Descomentar si quieres limpiar, pero ¡cuidado con las FKs!
        # cursor.execute("DELETE FROM RESENA") 
        # cursor.execute("DELETE FROM TRANSACCION")
        # cursor.execute("DELETE FROM SERVICIO") 
        # cursor.execute("DELETE FROM DEPENDIENTE")
        # cursor.execute("DELETE FROM USUARIA") 
        # cursor.execute("DELETE FROM CUIDADOR") 
        # cursor.execute("DELETE FROM CENTRO") 
        
        # 1. Insertar USUARIOS
        for u in USUARIOS_DATA:
            sql_u = f"INSERT INTO USUARIA VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
            cursor.execute(sql_u, (u['ID'], u['Nombre'], u['Apellido'], u['DNI'], u['FechaNacimiento'], u['Barrio'], u['Renta'], u['Telefono'], u['Email'], u['Genero']))

        # 2. Insertar DEPENDIENTES
        for d in DEPENDIENTES_DATA:
            sql_d = f"INSERT INTO DEPENDIENTE VALUES (%s, %s, %s, %s, %s, %s, %s)"
            cursor.execute(sql_d, (d['ID'], d['IDUsuario'], d['Nombre'], d['Apellido'], d['TipoDependencia'], d['FechaNacimiento'], d['PerfilMedico']))
        
        # 3. Insertar CUIDADORES
        for c in CUIDADORES_DATA:
            sql_c = f"INSERT INTO CUIDADOR VALUES (%s, %s, %s, %s, %s, %s, %s)"
            cursor.execute(sql_c, (c['ID'], c['Nombre'], c['Apellido'], c['DNI'], c['Telefono'], c['Disponibilidad'], c['Especialidad']))

        # 4. Insertar CENTROS
        for ce in CENTROS_DATA:
            sql_ce = f"INSERT INTO CENTRO VALUES (%s, %s, %s, %s, %s)"
            cursor.execute(sql_ce, (ce['ID'], ce['NombreCentro'], ce['Direccion'], ce['DescripcionCentro'], ce['CapacidadMaxima']))


        # 5. Insertar SERVICIOS, TRANSACCIONES y RESEÑAS
        servicio_ids = []
        for i in range(NUM_SERVICIOS):
            u_id = random.randint(1, NUM_BASE_RECORDS)
            # Encuentra el dependiente asociado a ese usuario (simplificado para que siempre haya uno)
            d_id = next(d['ID'] for d in DEPENDIENTES_DATA if d['IDUsuario'] == u_id) 
            c_id = random.choice([None, random.randint(1, NUM_BASE_RECORDS)])
            ce_id = random.choice([None, random.randint(1, len(CENTROS_DATA))])
            
            start = fake.date_time_this_year()
            end = start + timedelta(hours=random.randint(1, 8))
            
            precio_base = round(random.uniform(20.00, 100.00), 2)
            subvencion = round(precio_base * random.choice([0.0, 0.1, 0.2]), 2)
            precio_final = precio_base - subvencion
            estado = random.choice(['Completado', 'Completado', 'Completado', 'Asignado', 'Cancelado'])
            
            # Insertar SERVICIO
            sql_s = f"INSERT INTO SERVICIO (IDUsuario, IDDependiente, IDCuidador, IDCentro, FechaHoraInicio, FechaHoraFin, PrecioBase, SubvencionAplicada, PrecioFinal, Estado) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
            cursor.execute(sql_s, (u_id, d_id, c_id, ce_id, start, end, precio_base, subvencion, precio_final, estado))
            
            # Obtener el ID del servicio recién insertado
            if db_type == 'mysql':
                servicio_id = cursor.lastrowid
            else: # PostgreSQL
                cursor.execute("SELECT lastval()")
                servicio_id = cursor.fetchone()[0]
            servicio_ids.append(servicio_id)

            # Insertar TRANSACCIÓN (si no está cancelado)
            if estado != 'Cancelado':
                estado_pago = random.choice(['Completado', 'Completado', 'Pendiente', 'Fallido'])
                sql_t = f"INSERT INTO TRANSACCION (IDServicio, Monto, EstadoPago) VALUES (%s, %s, %s)"
                cursor.execute(sql_t, (servicio_id, precio_final, estado_pago))
            
            # Insertar RESEÑA (si está completado)
            if estado == 'Completado' and random.random() < 0.7:
                puntuacion = random.randint(1, 5)
                sql_r = f"INSERT INTO RESENA (IDServicio, IDUsuario, Puntuacion, Comentario) VALUES (%s, %s, %s, %s)"
                cursor.execute(sql_r, (servicio_id, u_id, puntuacion, fake.text(max_nb_chars=100)))

        conn.commit()
        print(f"  {db_type.upper()}: {NUM_SERVICIOS} servicios y transacciones/reseñas asociadas insertados.")

    except (mysql.connector.Error, psycopg2.Error) as err:
        print(f"  {db_type.upper()} ERROR: {err}")
    finally:
        if conn: conn.close()

def populate_mongodb(usuarios, cuidadores):
    print("-> Rellenando MongoDB...")
    try:
        client = MongoClient(DB_CONFIG['mongo_uri'])
        db = client[DB_NAME]
        
        # 1. Insertar USUARIOS (JSON enriquecido)
        usuarios_mongo = [{
            'IDUsuario': u['ID'],
            'NombreCompleto': f"{u['Nombre']} {u['Apellido']}",
            'DatosSensibles': {'Genero': u['Genero'], 'Barrio': u['Barrio'], 'RentaPercapita': float(u['Renta'])},
            'Contacto': {'email': u['Email'], 'Telefono': u['Telefono']}
        } for u in usuarios]
        db['usuarios_mongo'].insert_many(usuarios_mongo)

        # 2. Insertar CUIDADORES (para referencia)
        cuidadores_mongo = [{'IDCuidador': c['ID'], 'NombreCompleto': f"{c['Nombre']} {c['Apellido']}", 'Especialidad': c['Especialidad']} for c in cuidadores]
        db['cuidadores_mongo'].insert_many(cuidadores_mongo)
        
        # Nota: Para el análisis en MongoDB, almacenaríamos los servicios y reseñas como documentos anidados.

        client.close()
        print(f"  MongoDB: {NUM_BASE_RECORDS} documentos por colección base insertados.")
        
    except Exception as e:
        print(f"  MongoDB ERROR: {e}")

if __name__ == '__main__':
    print("--- INICIO DE POBLACIÓN DE BASES DE DATOS CON FAKER ---")
    
    # 1. Poblar MySQL
    populate_sql('mysql', DB_CONFIG['mysql'])
    
    # 2. Poblar PostgreSQL
    populate_sql('postgres', DB_CONFIG['postgres'])
    
    # 3. Poblar MongoDB
    populate_mongodb(USUARIOS_DATA, CUIDADORES_DATA)