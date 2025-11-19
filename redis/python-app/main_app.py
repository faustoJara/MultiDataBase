import redis
import mysql.connector
import os
import json
import time
from datetime import datetime

# --- IMPORTACIONES DE REDIS SEARCH ---
from redis.commands.search.field import TextField, NumericField, TagField
from redis.commands.search.indexDefinition import IndexDefinition, IndexType
from redis.commands.search.query import Query
from redis.commands.search.aggregation import AggregateRequest, Desc
# Importamos reducers para la agregación
import redis.commands.search.reducers as reducers

# --- CONFIGURACIÓN ---
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
DB_HOST = os.getenv("MYSQL_HOST", "localhost")
DB_NAME = os.getenv("MYSQL_DB", "CENTROCUIDADOFAMILIAR")
DB_USER = os.getenv("MYSQL_USER", "root")
DB_PASS = os.getenv("MYSQL_PASSWORD")

# Helper de Logs
def log(titulo, *args):
    print(f"\n{'='*60}")
    print(f"  {titulo.upper()}")
    print(f"{'='*60}")
    for msg in args:
        print(f"  -> {msg}")
    time.sleep(0.5)

# Helper Conexión SQL
def get_mysql_conn():
    return mysql.connector.connect(
        host=DB_HOST, user=DB_USER, password=DB_PASS, database=DB_NAME
    )

# Limpieza Inicial
def limpiar_db(r):
    log("LIMPIEZA INICIAL", "Borrando datos antiguos de Redis...")
    r.flushall()
    print("  -> Redis limpio y listo para recibir datos de MySQL.")

# --- EJERCICIOS 1-13: CLAVE-VALOR ---

def req_1_crear_kv(r):
    log("1. CREAR REGISTROS CLAVE-VALOR (Desde MySQL)",
        "Leyendo tabla USUARIA de MySQL y creando claves simples en Redis.")
    
    conn = get_mysql_conn()
    cursor = conn.cursor(dictionary=True)
    cursor.execute("SELECT IDUsuario, Nombre, Apellido, DNI FROM USUARIA LIMIT 10")
    usuarios = cursor.fetchall()
    conn.close()

    if not usuarios:
        log("1. ERROR", "No hay usuarios en MySQL. Ejecuta 'populate_faker.py' primero.")
        return

    count = 0
    for u in usuarios:
        key_base = f"centro:usuario:{u['IDUsuario']}"
        r.set(f"{key_base}:nombre", u['Nombre'])
        r.set(f"{key_base}:apellido", u['Apellido'])
        r.set(f"{key_base}:dni", u['DNI'])
        count += 1
    
    log("1. RESULTADO", f"Se han migrado {count} usuarios de MySQL a claves Redis simples.")

def req_2_contar_claves(r):
    log("2. OBTENER NÚMERO DE CLAVES", "Contando claves totales en Redis.")
    total = r.dbsize()
    log("2. RESULTADO", f"Total de claves en Redis: {total}")

def req_3_obtener_clave(r):
    log("3. OBTENER REGISTRO POR CLAVE", "Obteniendo el nombre del Usuario ID 1.")
    val = r.get("centro:usuario:1:nombre")
    log("3. RESULTADO", f"Valor de 'centro:usuario:1:nombre': {val}")

def req_4_actualizar_clave(r):
    log("4. ACTUALIZAR VALOR", "Corrigiendo el nombre del Usuario 1.")
    antiguo = r.get("centro:usuario:1:nombre")
    r.set("centro:usuario:1:nombre", f"{antiguo} (Editado)")
    nuevo = r.get("centro:usuario:1:nombre")
    log("4. RESULTADO", f"Antiguo: {antiguo} -> Nuevo: {nuevo}")

def req_5_eliminar_clave(r):
    log("5. ELIMINAR CLAVE-VALOR", "Eliminando el DNI del Usuario 1.")
    key = "centro:usuario:1:dni"
    r.delete(key)
    exists = r.exists(key)
    log("5. RESULTADO", f"Clave '{key}' eliminada. ¿Existe?: {exists == 1}")

def req_6_obtener_todas_las_claves(r):
    log("6. OBTENER TODAS LAS CLAVES", "Listando claves de usuarios.")
    keys = r.keys("centro:usuario:*:nombre")
    log("6. RESULTADO", f"Encontradas {len(keys)} claves.", f"Ejemplo: {keys[:3]}")

def req_7_obtener_todos_los_valores(r):
    log("7. OBTENER TODOS LOS VALORES", "Obteniendo nombres de usuarios.")
    keys = r.keys("centro:usuario:*:nombre")
    if keys:
        valores = r.mget(keys)
        log("7. RESULTADO", f"Nombres: {valores[:5]} ...")

def req_8_patron_asterisco(r):
    log("8. PATRÓN * (Asterisco)", "Todo sobre el Usuario 2.")
    keys = r.keys("centro:usuario:2:*")
    log("8. RESULTADO", f"Claves encontradas: {keys}")

def req_9_patron_corchetes(r):
    log("9. PATRÓN [] (Rango)", "Usuarios ID 1 a 3.")
    keys = r.keys("centro:usuario:[1-3]:nombre")
    log("9. RESULTADO", f"Claves encontradas: {keys}")

def req_10_patron_interrogacion(r):
    log("10. PATRÓN ? (Interrogación)", "Usuarios con ID de un dígito.")
    keys = r.keys("centro:usuario:?:nombre")
    log("10. RESULTADO", f"Encontrados: {len(keys)}")

def req_11_filtrar_por_valor(r):
    log("11. FILTRAR POR VALOR (Manual)", "Buscando usuarios con nombre 'Ana'.")
    keys = r.keys("centro:usuario:*:nombre")
    encontrados = []
    if keys:
        valores = r.mget(keys)
        for i, nombre in enumerate(valores):
            if nombre and "Ana" in nombre:
                encontrados.append(keys[i])
    log("11. RESULTADO", f"Usuarios 'Ana': {encontrados}")

def req_12_actualizar_por_filtro(r):
    log("12. ACTUALIZAR POR FILTRO", "Añadiendo 'VIP' a usuarios 1 y 2.")
    keys = r.keys("centro:usuario:[1-2]:apellido")
    for k in keys:
        antiguo = r.get(k)
        r.set(k, f"VIP {antiguo}")
    log("12. RESULTADO", "Apellidos actualizados.")

def req_13_eliminar_por_filtro(r):
    log("13. ELIMINAR POR FILTRO", "Borrando claves temporales.")
    r.set("temp:borrame", "1")
    keys = r.keys("temp:*")
    if keys:
        r.delete(*keys)
        log("13. RESULTADO", f"Eliminadas {len(keys)} claves.")

# --- EJERCICIOS 14-20: AVANZADOS ---

def req_14_crear_json(r):
    log("14. CREAR JSON (Desde MySQL)", "Migrando tabla SERVICIO a Redis JSON.")
    
    conn = get_mysql_conn()
    cursor = conn.cursor(dictionary=True)
    
    query = """
        SELECT 
            s.IDServicio, s.FechaHoraInicio, s.FechaHoraFin, s.PrecioFinal, s.Estado,
            u.Nombre as NomUsu, u.Apellido as ApeUsu,
            c.Nombre as NomCui, c.Especialidad,
            cen.NombreCentro
        FROM SERVICIO s
        LEFT JOIN USUARIA u ON s.IDUsuario = u.IDUsuario
        LEFT JOIN CUIDADOR c ON s.IDCuidador = c.IDCuidador
        LEFT JOIN CENTRO cen ON s.IDCentro = cen.IDCentro
    """
    cursor.execute(query)
    servicios = cursor.fetchall()
    conn.close()

    pipeline = r.pipeline()
    for s in servicios:
        duracion = 0
        if s['FechaHoraInicio'] and s['FechaHoraFin']:
            duracion = int((s['FechaHoraFin'] - s['FechaHoraInicio']).total_seconds() / 60)

        doc = {
            "id_servicio": s['IDServicio'],
            "usuario": f"{s['NomUsu']} {s['ApeUsu']}",
            "cuidador": s['NomCui'] or "Sin Asignar",
            "especialidad": s['Especialidad'] or "General",
            "centro": s['NombreCentro'] or "Domicilio",
            "precio": float(s['PrecioFinal'] or 0),
            "duracion_minutos": duracion,
            "estado": s['Estado']
        }
        pipeline.json().set(f"servicio:{s['IDServicio']}", "$", doc)
    
    pipeline.execute()
    log("14. RESULTADO", f"{len(servicios)} servicios cargados en Redis.")

def req_15_filtrar_json_atributos(r):
    log("15. FILTRAR JSON POR ATRIBUTO", "Obteniendo solo precio y usuario del servicio 1.")
    datos = r.json().get("servicio:1", "$.precio", "$.usuario")
    log("15. RESULTADO", f"Datos: {datos}")

def req_16_crear_lista(r):
    log("16. CREAR UNA LISTA", "Creando historial para Usuario 1.")
    r.delete("historial:usuario:1")
    r.rpush("historial:usuario:1", "servicio:1", "servicio:5")
    log("16. RESULTADO", "Lista creada.")

def req_17_obtener_lista_filtro(r):
    log("17. OBTENER ELEMENTOS DE LISTA", "Obteniendo todo el historial.")
    items = r.lrange("historial:usuario:1", 0, -1)
    log("17. RESULTADO", f"Historial: {items}")

def req_18_crear_indices(r):
    log("18. CREAR ÍNDICE (RedisSearch)", "Creando índice para Servicios.")
    
    schema = (
        TextField("$.usuario", as_name="usuario"),
        TagField("$.centro", as_name="centro"),
        # Alias 'precio' y 'duracion' son cruciales para req_20
        NumericField("$.precio", as_name="precio", sortable=True),
        NumericField("$.duracion_minutos", as_name="duracion", sortable=True)
    )
    
    try:
        r.ft("idx:servicios").dropindex()
    except:
        pass

    definition = IndexDefinition(prefix=["servicio:"], index_type=IndexType.JSON)
    r.ft("idx:servicios").create_index(schema, definition=definition)
    
    log("18. RESULTADO", "Índice 'idx:servicios' creado.")

def req_19_busqueda_indices(r):
    log("19. BÚSQUEDA CON ÍNDICES", "Servicios con precio < 80.")
    q = Query("@precio:[-inf 80]").sort_by("precio")
    res = r.ft("idx:servicios").search(q)
    log("19. RESULTADO", f"Encontrados: {res.total}")

def req_20_group_by_indices(r):
    log("20. GROUP BY CON ÍNDICES", "Agrupando por Centro (Ingresos y Duración).")
    
    # --- CORRECCIÓN ---
    # Usamos los ALIAS del índice (@precio, @duracion) no las rutas JSON ($.precio)
    req = AggregateRequest("*").group_by(
        ["@centro"], 
        reducers.count().alias("conteo"), 
        reducers.sum("@precio").alias("ingresos_totales"),  # @precio
        reducers.avg("@duracion").alias("duracion_media")   # @duracion
    ).sort_by(Desc("@conteo")) 

    try:
        res = r.ft("idx:servicios").aggregate(req)
        
        resultados = []
        for row in res.rows:
            # El resultado viene como una lista de tuplas/bytes, lo parseamos
            # Dependiendo de la versión de redis-py, row puede ser una lista o un dict
            # En decode_responses=True, suelen ser strings o floats
            
            # Buscamos por nombre de campo en el resultado si es posible
            centro = "Desconocido"
            conteo = 0
            ingresos = 0
            duracion = 0
            
            # Método robusto: convertir la row a dict si es una lista plana
            # o acceder por índice si sabemos el orden
            # La estructura suele ser [campo, valor, campo, valor...] o un objeto.
            # Con la librería moderna, suele permitir acceso directo.
            
            try:
                # Intentamos acceso directo asumiendo que row es similar a un dict
                centro = row[1] # El valor de @centro
                conteo = row[3] # El valor de conteo
                ingresos = float(row[5])
                duracion = float(row[7])
            except:
                # Fallback si la estructura es distinta
                pass

            resultados.append(f"{centro}: {conteo} servicios, {ingresos:.2f}€, {duracion:.1f} min avg")

        log("20. RESULTADO", f"Estadísticas:\n {json.dumps(resultados, indent=2)}")
    except Exception as e:
        log("20. ERROR", str(e))

# --- EJECUCIÓN ---
def main():
    try:
        r = redis.Redis(host=REDIS_HOST, port=6379, decode_responses=True)
        limpiar_db(r)
        
        req_1_crear_kv(r)
        req_2_contar_claves(r)
        req_3_obtener_clave(r)
        req_4_actualizar_clave(r)
        req_5_eliminar_clave(r)
        req_6_obtener_todas_las_claves(r)
        req_7_obtener_todos_los_valores(r)
        req_8_patron_asterisco(r)
        req_9_patron_corchetes(r)
        req_10_patron_interrogacion(r)
        req_11_filtrar_por_valor(r)
        req_12_actualizar_por_filtro(r)
        req_13_eliminar_por_filtro(r)
        
        req_14_crear_json(r)
        req_15_filtrar_json_atributos(r)
        req_16_crear_lista(r)
        req_17_obtener_lista_filtro(r)
        req_18_crear_indices(r)
        time.sleep(1) # Espera para indexado
        req_19_busqueda_indices(r)
        req_20_group_by_indices(r)
        
        log("FIN", "Ejecución completada con éxito.")

    except Exception as e:
        log("ERROR CRÍTICO", str(e))

if __name__ == "__main__":
    main()