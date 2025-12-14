import mysql.connector
import time
import sys
import os
from dotenv import load_dotenv

# === CARGAR VARIABLES DEL .ENV ===
load_dotenv()

# Obtenemos las variables o usamos valores por defecto/error
RDS_HOST = os.getenv("RDS_HOST")
RDS_USER = os.getenv("RDS_USER")
RDS_PASS = os.getenv("RDS_PASS")
DB_NAME = os.getenv("RDS_DB_NAME", "CentroCuidadoNube")

if not RDS_HOST or not RDS_PASS:
    print("ERROR: No se encontraron las variables RDS_HOST o RDS_PASS en el archivo .env")
    sys.exit(1)

def log(titulo, mensaje):
    print(f"\n{'='*60}\n  {titulo}\n{'='*60}")
    print(f" -> {mensaje}")

def conectar_server():
    return mysql.connector.connect(
        host=RDS_HOST, user=RDS_USER, password=RDS_PASS
    )

def conectar_db():
    return mysql.connector.connect(
        host=RDS_HOST, user=RDS_USER, password=RDS_PASS, database=DB_NAME
    )

def gestionar_rds():
    log("INICIO RDS", f"Conectando a {RDS_HOST}...")
    
    # 1. Crear Base de Datos
    try:
        conn = conectar_server()
        cursor = conn.cursor()
        cursor.execute(f"CREATE DATABASE IF NOT EXISTS {DB_NAME}")
        conn.commit()
        log("1. CREAR DB", f"Base de datos '{DB_NAME}' verificada/creada.")
        conn.close()
    except Exception as e:
        print(f"Error conectando al servidor RDS: {e}")
        return

    # 2. Crear Tablas e Insertar Datos
    try:
        conn = conectar_db()
        cursor = conn.cursor()
        
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS ReportesNube (
                ID INT AUTO_INCREMENT PRIMARY KEY,
                Tipo VARCHAR(50),
                Descripcion VARCHAR(255),
                Fecha TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        log("2. CREAR TABLA", "Tabla 'ReportesNube' creada.")

        cursor.execute("SELECT COUNT(*) FROM ReportesNube")
        if cursor.fetchone()[0] == 0:
            datos = [
                ('Mensual', 'Reporte de ingresos enero'),
                ('Incidencia', 'Caída del servidor principal'),
                ('Auditoria', 'Revisión anual completada'),
                ('Mensual', 'Reporte de gastos febrero')
            ]
            cursor.executemany("INSERT INTO ReportesNube (Tipo, Descripcion) VALUES (%s, %s)", datos)
            conn.commit()
            log("3. INSERTAR", "Registros insertados en RDS.")
        else:
            log("3. INSERTAR", "La tabla ya tenía datos.")

        # 4. Realizar 3 Consultas
        log("CONSULTA 1", "Obtener todos los reportes:")
        cursor.execute("SELECT * FROM ReportesNube")
        for row in cursor.fetchall():
            print(f"  Row: {row}")

        log("CONSULTA 2", "Filtrar por Tipo = 'Mensual':")
        cursor.execute("SELECT Descripcion, Fecha FROM ReportesNube WHERE Tipo = 'Mensual'")
        for row in cursor.fetchall():
            print(f"  Mensual: {row}")

        log("CONSULTA 3", "Contar total:")
        cursor.execute("SELECT COUNT(*) FROM ReportesNube")
        total = cursor.fetchone()[0]
        print(f"  Total: {total}")

    except Exception as e:
        print(f"Error RDS: {e}")
    finally:
        if 'conn' in locals() and conn.is_connected():
            conn.close()

if __name__ == "__main__":
    gestionar_rds()