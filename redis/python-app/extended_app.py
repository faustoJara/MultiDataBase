import redis
import mysql.connector
import os
import time
from datetime import datetime
from redis.commands.search.field import TextField, NumericField, TagField
from redis.commands.search.indexDefinition import IndexDefinition, IndexType
from redis.commands.search.query import Query

# === CONFIGURACIÓN ===
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
DB_HOST = os.getenv("MYSQL_HOST", "localhost")
DB_NAME = os.getenv("MYSQL_DB", "CENTROCUIDADOFAMILIAR")
DB_USER = os.getenv("MYSQL_USER", "root")
DB_PASS = os.getenv("MYSQL_PASSWORD")

def log(titulo, msg):
    print(f"\n[{titulo}] {msg}")

def conectar():
    # Conectamos a Redis (decodificando respuestas para tener strings)
    r = redis.Redis(host=REDIS_HOST, port=6379, decode_responses=True)
    # Conectamos a MySQL
    conn = mysql.connector.connect(
        host=DB_HOST, user=DB_USER, password=DB_PASS, database=DB_NAME
    )
    return r, conn

# ---------------------------------------------------------
# PASO 1: CARGA  DE DATOS (MySQL -> Redis)
# ---------------------------------------------------------
def paso_1_cargar_datos(r, conn):
    log("PASO 1", "Extrayendo de MySQL y cargando en Redis...")
    cursor = conn.cursor(dictionary=True)
    
    # Obtenemos datos crudos de MySQL
    cursor.execute("""
        SELECT 
            s.IDServicio, u.Nombre as NomUsu, c.Nombre as NomCui, 
            cen.NombreCentro, s.FechaHoraInicio, s.FechaHoraFin
        FROM SERVICIO s
        LEFT JOIN USUARIA u ON s.IDUsuario = u.IDUsuario
        LEFT JOIN CUIDADOR c ON s.IDCuidador = c.IDCuidador
        LEFT JOIN CENTRO cen ON s.IDCentro = cen.IDCentro
        WHERE s.FechaHoraFin IS NOT NULL
    """)
    servicios = cursor.fetchall()
    
    if not servicios:
        print("⚠️ No hay datos en MySQL para cargar.")
        return 0

    pipeline = r.pipeline()
    count = 0
    
    for serv in servicios:
        # Transformación (Python): Calcular duración
        inicio = serv['FechaHoraInicio']
        fin = serv['FechaHoraFin']
        minutos = int((fin - inicio).total_seconds() / 60) if fin > inicio else 0
        
        # Clave única para Redis
        key = f"datos_mysql:servicio:{serv['IDServicio']}"
        
        # Estructura para Redis (Hash)
        data = {
            "id": serv['IDServicio'],
            "usuario": serv['NomUsu'],
            "cuidador": serv['NomCui'],
            "centro": serv['NombreCentro'] if serv['NombreCentro'] else "Domicilio",
            "duracion": minutos
        }
        
        # Convertimos todo a string para Redis
        mapping = {k: str(v) for k, v in data.items()}
        pipeline.hset(key, mapping=mapping)
        count += 1
        
    pipeline.execute()
    log("REDIS", f"✅ {count} registros cargados en Redis bajo 'datos_mysql:servicio:*'.")
    return count

# ---------------------------------------------------------
# PASO 2: INDEXACIÓN (Preparar Redis para consultas)
# ---------------------------------------------------------
def paso_2_crear_indice(r):
    log("PASO 2", "Creando Índice en Redis para poder hacer consultas...")
    index_name = "idx:tiempos_servicio"
    
    # Definimos el esquema de los datos que acabamos de cargar
    schema = (
        NumericField("duracion", sortable=True),
        TextField("centro", sortable=True),
        TextField("usuario", sortable=True)
    )
    
    definition = IndexDefinition(prefix=["datos_mysql:servicio:"], index_type=IndexType.HASH)

    try:
        # Intentamos borrar el índice si ya existe para empezar limpio
        try:
            r.ft(index_name).dropindex()
        except:
            pass
            
        # Creamos el índice
        r.ft(index_name).create_index(schema, definition=definition)
        log("REDIS SEARCH", f"✅ Índice '{index_name}' creado. Ahora Redis puede consultar sus propios datos.")
    except Exception as e:
        print(f"Error creando índice: {e}")

# ---------------------------------------------------------
# PASO 3: REDIS HACE EL TRABAJO PARA LA MIGRACIÓN
# ---------------------------------------------------------
def paso_3_consultar_y_migrar(r, conn):
    log("PASO 3", "Redis consulta sus datos y migramos el resultado a MySQL...")
    
    # === AQUÍ ESTÁ LA CLAVE DE TU PREGUNTA ===
    # En lugar de migrar todo ciegamente, le pedimos a Redis que filtre.
    # Consulta: "Dáme todos los servicios con duración mayor a 0 minutos, ordenados por duración descendente"
    
    q = Query("@duracion:[(0 +inf]").sort_by("duracion", asc=False)
    
    # Ejecutamos la consulta contra la base de datos de Redis
    res = r.ft("idx:tiempos_servicio").search(q)
    
    print(f" -> Redis encontró {res.total} servicios válidos (duración > 0).")
    
    if res.total == 0:
        return

    # Preparamos los datos para MySQL
    datos_finales = []
    for doc in res.docs:
        # doc es el objeto que Redis nos devuelve tras su consulta interna
        datos_finales.append((
            doc.id.split(":")[-1], # ID original extraído de la clave
            doc.usuario,
            doc.cuidador,
            doc.centro,
            doc.duracion
        ))

    # Escribimos en la tabla final de MySQL
    cursor = conn.cursor()
    sql_insert = """
        INSERT INTO REGISTRO_TIEMPO 
        (IDServicioOriginal, NombreUsuario, NombreCuidador, NombreCentro, DuracionMinutos, FechaCalculo)
        VALUES (%s, %s, %s, %s, %s, NOW())
        ON DUPLICATE KEY UPDATE DuracionMinutos = VALUES(DuracionMinutos), FechaCalculo = NOW()
    """
    
    try:
        cursor.executemany(sql_insert, datos_finales)
        conn.commit()
        log("MYSQL", f"✅ {cursor.rowcount} registros (filtrados por Redis) insertados en REGISTRO_TIEMPO.")
        
        # Muestra final
        print("\n--- Muestra de la Tabla Final en MySQL ---")
        cursor.execute("SELECT * FROM REGISTRO_TIEMPO LIMIT 5")
        for row in cursor.fetchall():
            print(row)
            
    except mysql.connector.Error as err:
        print(f"Error MySQL: {err}")

def main():
    r, conn = None, None
    try:
        r, conn = conectar()
        
        # Flujo completo
        if paso_1_cargar_datos(r, conn) > 0:
            paso_2_crear_indice(r)
            # Esperamos un micro-segundo para asegurar que el índice se actualice
            time.sleep(0.5) 
            paso_3_consultar_y_migrar(r, conn)
        
    except Exception as e:
        print(f"\n❌ ERROR CRÍTICO: {e}")
    finally:
        if conn: conn.close()
        if r: r.close()

if __name__ == "__main__":
    main()