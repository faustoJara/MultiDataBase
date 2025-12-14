import mysql.connector
import time

# === CONFIGURACIÓN RDS ===
# ¡SUSTITUYE ESTO CON TUS DATOS DEL LABORATORIO AWS!
RDS_HOST = "database-1.cluster-ro-xxxxxx.us-east-1.rds.amazonaws.com" 
RDS_USER = "admin"
RDS_PASS = "tu_password_rds"
DB_NAME = "CentroCuidadoNube" # La base de datos que vamos a crear

def log(titulo, mensaje):
    print(f"\n{'='*50}\n  {titulo}\n{'='*50}")
    print(f" -> {mensaje}")

def conectar_server():
    # Conexión al servidor (sin base de datos específica) para poder crearla
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
    conn = conectar_server()
    cursor = conn.cursor()
    try:
        cursor.execute(f"CREATE DATABASE IF NOT EXISTS {DB_NAME}")
        log("CREAR DB", f"Base de datos '{DB_NAME}' verificada/creada.")
    except Exception as e:
        print(f"Error creando DB: {e}")
    finally:
        conn.close()

    # 2. Crear Tablas e Insertar Datos
    conn = conectar_db()
    cursor = conn.cursor()
    try:
        # Crear tabla
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS ReportesNube (
                ID INT AUTO_INCREMENT PRIMARY KEY,
                Tipo VARCHAR(50),
                Descripcion VARCHAR(255),
                Fecha TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        log("CREAR TABLA", "Tabla 'ReportesNube' creada.")

        # Insertar datos
        datos = [
            ('Mensual', 'Reporte de ingresos enero'),
            ('Incidencia', 'Caída del servidor principal'),
            ('Auditoria', 'Revisión anual completada')
        ]
        cursor.executemany("INSERT INTO ReportesNube (Tipo, Descripcion) VALUES (%s, %s)", datos)
        conn.commit()
        log("INSERTAR", "3 registros insertados en RDS.")

        # 3. Realizar 3 Consultas
        log("CONSULTA 1", "Obtener todos los reportes:")
        cursor.execute("SELECT * FROM ReportesNube")
        for row in cursor.fetchall():
            print(f"  Row: {row}")

        log("CONSULTA 2", "Filtrar por Tipo = 'Incidencia':")
        cursor.execute("SELECT Descripcion, Fecha FROM ReportesNube WHERE Tipo = 'Incidencia'")
        for row in cursor.fetchall():
            print(f"  Incidencia: {row}")

        log("CONSULTA 3", "Contar total de reportes:")
        cursor.execute("SELECT COUNT(*) FROM ReportesNube")
        total = cursor.fetchone()[0]
        print(f"  Total: {total}")

    except Exception as e:
        print(f"Error RDS: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    gestionar_rds()