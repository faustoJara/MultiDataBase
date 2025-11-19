import redis
import mysql.connector
import os
import json
import sys
from redis.commands.search.query import Query

# --- Configuración de Conexiones ---
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")

# Leemos las variables de entorno (pasadas por docker-compose)
DB_HOST = os.getenv("MYSQL_HOST", "localhost")
DB_NAME = os.getenv("MYSQL_DB", "CENTROCUIDADOFAMILIAR")
DB_USER = os.getenv("MYSQL_USER", "root")
DB_PASS = os.getenv("MYSQL_PASSWORD")

# Función de ayuda para imprimir logs
def log(titulo, *args):
    print(f"\n{'='*50}")
    # CORREGIDO: .upper() en minúsculas
    print(f"  {titulo.upper()}") 
    print(f"{'='*50}")
    for msg in args:
        print(f"  -> {msg}")
    print(f"{'='*50}")

# --- Conexiones ---
try:
    # Conectar a Redis
    r = redis.Redis(host=REDIS_HOST, port=6379, decode_responses=True)
    r.ping()
    print("Conexión a Redis (para SQL) exitosa.")
except Exception as e:
    print(f"Error conectando a Redis: {e}")
    sys.exit(1)

if not DB_PASS:
    print("Error: La variable de entorno MYSQL_PASSWORD no está definida.")
    sys.exit(1)

try:
    # Conectar a MySQL
    conn_sql = mysql.connector.connect(
        host=DB_HOST,
        user=DB_USER,
        password=DB_PASS,
        database=DB_NAME
    )
    print(f"Conexión a MySQL ({DB_NAME} en {DB_HOST}) exitosa.")
except Exception as e:
    print(f"Error conectando a MySQL: {e}")
    print("Asegúrate de que 'setup_sql.py' se haya ejecutado y las variables de entorno sean correctas.")
    sys.exit(1)


# 21. Obtener datos de SQL e incluirlos en Redis
def req_21_sql_a_redis(r, conn):
    log("21. MIGRAR DATOS: MYSQL -> REDIS",
        "Leemos 'USUARIA' y 'CUIDADOR' de MySQL y los guardamos como JSON en Redis (Caché).")
    
    try:
        # Usamos dictionary=True para obtener resultados como diccionarios
        cursor = conn.cursor(dictionary=True)
        
        # --- Migrar USUARIAS ---
        cursor.execute("SELECT IDUsuario, Nombre, Apellido, Barrio, Email FROM USUARIA")
        usuarias_sql = cursor.fetchall()
        
        pipeline = r.pipeline()
        count_u = 0
        if not usuarias_sql:
            print("  -> No se encontraron USUARIAS en MySQL para migrar.")
        else:
            for usuaria in usuarias_sql:
                redis_key = f"usuaria:sql:{usuaria['IDUsuario']}"
                pipeline.json().set(redis_key, "$", usuaria)
                count_u += 1
        
        # --- Migrar CUIDADORES ---
        cursor.execute("SELECT IDCuidador, Nombre, Apellido, Especialidad FROM CUIDADOR")
        cuidadores_sql = cursor.fetchall()
        
        count_c = 0
        if not cuidadores_sql:
            print("  -> No se encontraron CUIDADORES en MySQL para migrar.")
        else:
            for cuidador in cuidadores_sql:
                # Convertimos tipos de datos si es necesario (JSON prefiere tipos nativos)
                redis_key = f"cuidador:sql:{cuidador['IDCuidador']}"
                pipeline.json().set(redis_key, "$", cuidador)
                count_c += 1
            
        pipeline.execute()
        
        if count_u == 0 and count_c == 0:
             log("21. RESULTADO", "No se encontraron datos en MySQL para migrar a Redis.")
             return

        log("21. RESULTADO", f"{count_u} USUARIAS migradas de MySQL a Redis.",
            f"{count_c} CUIDADORES migrados de MySQL a Redis.",
            f"Ejemplo clave: 'usuaria:sql:{usuarias_sql[0]['IDUsuario'] if usuarias_sql else 'N/A'}'")

    except Exception as e:
        log("21. ERROR", str(e))

# 22. Obtener datos de Redis e incluirlos en la BD SQL
def req_22_redis_a_sql(r, conn):
    log("22. MIGRAR DATOS: REDIS -> MYSQL",
        "Leemos productos de 'Panaderia' de Redis (Req 14) y los insertamos en una nueva tabla en MySQL.",
        "Usaremos el índice 'idx:productos' para encontrar los datos.")
    
    try:
        # 1. Buscar todos los documentos en el índice de productos
        # (Asumimos que main_app.py ya se ejecutó y creó 'idx:productos')
        res = r.ft("idx:productos").search(Query("@categoria:{Panaderia}").limit(0, 100))
        
        if res.total == 0:
            log("22. INFO", "No se encontraron datos de 'Panaderia' en 'idx:productos' de Redis.",
                "Asegúrate de haber ejecutado 'main_app.py' primero.")
            return

        # 2. Preparar datos para SQL
        datos_para_sql = []
        for doc in res.docs:
            data = json.loads(doc.json) # Cargamos el JSON string
            datos_para_sql.append((
                data['id_producto'], # Usamos id_producto como PK
                data['nombre'],
                data['categoria'],
                data['precio'],
                data['stock']
            ))

        # 3. Insertar o Actualizar en MySQL
        cursor = conn.cursor()
        # Creamos una tabla destino
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS redis_panaderia_migrados (
            id_producto VARCHAR(50) PRIMARY KEY,
            nombre TEXT,
            categoria TEXT,
            precio REAL,
            stock INTEGER
        )
        ''')
        
        # Usamos INSERT ... ON DUPLICATE KEY UPDATE (versión MySQL de "REPLACE")
        query_insert = """
        INSERT INTO redis_panaderia_migrados (id_producto, nombre, categoria, precio, stock)
        VALUES (%s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            nombre = VALUES(nombre),
            categoria = VALUES(categoria),
            precio = VALUES(precio),
            stock = VALUES(stock)
        """
        
        cursor.executemany(query_insert, datos_para_sql)
        conn.commit()
        
        log("22. RESULTADO", f"{len(datos_para_sql)} registros de 'Panaderia' migrados de Redis a MySQL.",
            "Los datos se encuentran en la tabla 'redis_panaderia_migrados'.")

    except Exception as e:
        log("22. ERROR", str(e))


# --- Ejecución Principal (SQL) ---
def main_sql():
    try:
        req_21_sql_a_redis(r, conn_sql)
        req_22_redis_a_sql(r, conn_sql)
    finally:
        if 'conn_sql' in locals() and conn_sql.is_connected():
            conn_sql.close()
            print("\nConexión MySQL cerrada.")
        if r:
            r.close()
            print("Conexión Redis cerrada.")

if __name__ == "__main__":
    main_sql()