import mysql.connector
import os
import random
from faker import Faker
from datetime import datetime, timedelta

# === CONFIGURACIÓN ===
# Usamos las variables de entorno que Docker inyecta
DB_HOST = os.getenv("MYSQL_HOST", "mysql")
DB_NAME = os.getenv("MYSQL_DB", "CENTROCUIDADOFAMILIAR")
DB_USER = os.getenv("MYSQL_USER", "root")
# Contraseña fija para asegurar conexión si la env falla o usamos la del docker-compose
DB_PASS = os.getenv("MYSQL_PASSWORD", "0853") 

NUM_BASE_RECORDS = 20 
NUM_SERVICIOS = 50
fake = Faker('es_ES')

def connect():
    return mysql.connector.connect(
        host=DB_HOST,
        user=DB_USER,
        password=DB_PASS,
        database=DB_NAME
    )

def main():
    print(f"--- Iniciando Población de Datos en {DB_HOST} ---")
    conn = None
    try:
        conn = connect()
        cursor = conn.cursor()

        print(" -> Limpiando tablas antiguas...")
        cursor.execute("SET FOREIGN_KEY_CHECKS = 0;")
        tablas = ["RESENA", "TRANSACCION", "SERVICIO", "DEPENDIENTE", "CUIDADOR", "CENTRO", "USUARIA", "REGISTRO_TIEMPO"]
        for t in tablas:
            try:
                cursor.execute(f"TRUNCATE TABLE {t}")
            except:
                pass
        cursor.execute("SET FOREIGN_KEY_CHECKS = 1;")

        # 1. Insertar USUARIOS
        print(" -> Creando Usuarios...")
        usuarios_ids = []
        for _ in range(NUM_BASE_RECORDS):
            sql = "INSERT INTO USUARIA (Nombre, Apellido, DNI, FechaNacimiento, Barrio, RentaPercapita, Telefono, Email, Genero) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)"
            val = (fake.first_name(), fake.last_name(), fake.unique.ssn(), fake.date_of_birth(minimum_age=18, maximum_age=90), fake.city_suffix(), round(random.uniform(10000, 60000), 2), fake.phone_number(), fake.unique.email(), random.choice(['Femenino', 'Masculino']))
            cursor.execute(sql, val)
            usuarios_ids.append(cursor.lastrowid)

        # 2. Insertar DEPENDIENTES
        print(" -> Creando Dependientes...")
        dependientes_ids = []
        for uid in usuarios_ids:
            sql = "INSERT INTO DEPENDIENTE (IDUsuario, Nombre, Apellido, TipoDependencia, FechaNacimiento, PerfilMedico) VALUES (%s, %s, %s, %s, %s, %s)"
            val = (uid, fake.first_name(), fake.last_name(), random.choice(['Niño', 'Mayor', 'Otro']), fake.date_of_birth(minimum_age=1, maximum_age=90), fake.text(max_nb_chars=50))
            cursor.execute(sql, val)
            dependientes_ids.append(cursor.lastrowid)

        # 3. Insertar CUIDADORES
        print(" -> Creando Cuidadores...")
        cuidadores_ids = []
        for _ in range(NUM_BASE_RECORDS):
            sql = "INSERT INTO CUIDADOR (Nombre, Apellido, DNI, Telefono, Disponibilidad, Especialidad) VALUES (%s, %s, %s, %s, %s, %s)"
            val = (fake.first_name(), fake.last_name(), fake.unique.ssn(), fake.phone_number(), random.choice(['Mañana', 'Tarde', 'Completa']), random.choice(['Geriatría', 'Infantil', 'Fisioterapia']))
            cursor.execute(sql, val)
            cuidadores_ids.append(cursor.lastrowid)

        # 4. Insertar CENTROS
        print(" -> Creando Centros...")
        centros_ids = []
        for _ in range(5):
            sql = "INSERT INTO CENTRO (NombreCentro, Direccion, DescripcionCentro, CapacidadMaxima) VALUES (%s, %s, %s, %s)"
            val = (fake.company(), fake.address(), fake.catch_phrase(), random.randint(10, 100))
            cursor.execute(sql, val)
            centros_ids.append(cursor.lastrowid)

        # 5. Insertar SERVICIOS (Crucial para el ejercicio de tiempo)
        print(f" -> Creando {NUM_SERVICIOS} Servicios con tiempos...")
        for _ in range(NUM_SERVICIOS):
            u_id = random.choice(usuarios_ids)
            d_id = random.choice(dependientes_ids) 
            c_id = random.choice(cuidadores_ids)
            ce_id = random.choice(centros_ids)
            
            # Generar fechas: Inicio hace poco, Fin unas horas después
            inicio = fake.date_time_between(start_date='-1y', end_date='now')
            duracion_horas = random.randint(1, 5)
            fin = inicio + timedelta(hours=duracion_horas)
            
            precio = round(random.uniform(20.00, 100.00), 2)
            
            sql = """
                INSERT INTO SERVICIO 
                (IDUsuario, IDDependiente, IDCuidador, IDCentro, FechaHoraInicio, FechaHoraFin, PrecioBase, PrecioFinal, Estado) 
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            val = (u_id, d_id, c_id, ce_id, inicio, fin, precio, precio, 'Finalizado')
            cursor.execute(sql, val)

        conn.commit()
        print(f"\n✅ ¡ÉXITO! {NUM_SERVICIOS} servicios insertados. Listo para Redis.")

    except mysql.connector.Error as err:
        print(f"\n❌ Error MySQL: {err}")
    finally:
        if conn: conn.close()

if __name__ == '__main__':
    main()