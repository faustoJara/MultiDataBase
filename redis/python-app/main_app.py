import redis
import os
import json
import time
from redis.commands.search.field import (
    TextField, NumericField, TagField
)
from redis.commands.search.indexDefinition import (
    IndexDefinition, IndexType
)
from redis.commands.search.query import Query
from redis.commands.search.aggregation import (
    AggregationRequest, AscSorting, DescSorting
)

# --- Configuración de la Conexión ---
# Obtenemos el host de Redis desde la variable de entorno
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")

# Función de ayuda para imprimir logs
def log(titulo, *args):
    print(f"\n{'='*50}")
    print(f"  {titulo.upper()}")
    print(f"{'='*50}")
    for msg in args:
        print(f"  -> {msg}")
    time.sleep(1) # Pausa para facilitar la lectura

# Función de ayuda para limpiar la BD
def limpiar_db(r):
    log("Limpiando Base de Datos", "Ejecutando FLUSHALL en Redis...")
    r.flushall()
    print("  -> Base de datos limpia.")

# --- Funciones de Requisitos (1-20) ---

# 1. Crear registros clave-valor
def req_1_crear_kv(r):
    log("1. CREAR REGISTROS CLAVE-VALOR",
        "Usamos SET para crear claves simples.",
        "Ej: SET tienda:producto:1001:nombre 'Café Colombia'",
        "Ej: SET tienda:producto:1001:stock 150")
    try:
        r.set("tienda:producto:1001:nombre", "Café Colombia")
        r.set("tienda:producto:1001:stock", 150)
        r.set("tienda:producto:1002:nombre", "Té Verde")
        r.set("tienda:producto:1002:stock", 90)
        r.set("tienda:producto:2001:nombre", "Leche de Almendras")
        r.set("tienda:producto:2001:stock", 75)
        r.set("tienda:categoria:Bebidas:1", "Café")
        r.set("tienda:categoria:Bebidas:2", "Té")
        r.set("tienda:categoria:Alternativos:1", "Leche Vegetal")
        log("1. RESULTADO", "6 claves creadas con éxito.")
    except Exception as e:
        log("1. ERROR", str(e))

# 2. Obtener y mostrar el número de claves registradas
def req_2_contar_claves(r):
    log("2. OBTENER NÚMERO DE CLAVES",
        "Usamos DBSIZE para contar todas las claves en la BD.")
    try:
        total_claves = r.dbsize()
        log("2. RESULTADO", f"Número total de claves: {total_claves}")
    except Exception as e:
        log("2. ERROR", str(e))

# 3. Obtener y mostrar un registro en base a una clave
def req_3_obtener_clave(r):
    log("3. OBTENER REGISTRO POR CLAVE",
        "Usamos GET para obtener el valor de una clave específica.",
        "Ej: GET tienda:producto:1001:nombre")
    try:
        valor = r.get("tienda:producto:1001:nombre")
        log("3. RESULTADO", f"Valor de 'tienda:producto:1001:nombre': {valor}")
    except Exception as e:
        log("3. ERROR", str(e))

# 4. Actualizar el valor de una clave
def req_4_actualizar_clave(r):
    log("4. ACTUALIZAR VALOR DE CLAVE",
        "Usamos SET nuevamente. Si la clave existe, sobrescribe el valor.",
        "Ej: SET tienda:producto:1002:stock 85")
    try:
        valor_antiguo = r.get("tienda:producto:1002:stock")
        r.set("tienda:producto:1002:stock", 85)
        valor_nuevo = r.get("tienda:producto:1002:stock")
        log("4. RESULTADO", 
            f"Valor antiguo de 'tienda:producto:1002:stock': {valor_antiguo}",
            f"Valor nuevo: {valor_nuevo}")
    except Exception as e:
        log("4. ERROR", str(e))

# 5. Eliminar una clave-valor
def req_5_eliminar_clave(r):
    log("5. ELIMINAR CLAVE-VALOR",
        "Usamos DEL para eliminar una o más claves.",
        "Ej: DEL tienda:categoria:Alternativos:1")
    try:
        clave_a_borrar = "tienda:categoria:Alternativos:1"
        valor_eliminado = r.get(clave_a_borrar)
        r.delete(clave_a_borrar)
        log("5. RESULTADO",
            f"Clave eliminada: {clave_a_borrar}",
            f"Valor que contenía: {valor_eliminado}")
    except Exception as e:
        log("5. ERROR", str(e))

# 6. Obtener y mostrar todas las claves guardadas
def req_6_obtener_todas_las_claves(r):
    log("6. OBTENER TODAS LAS CLAVES",
        "Usamos KEYS * para obtener todas las claves.",
        "¡PRECAUCIÓN! No usar 'KEYS *' en producción con muchas claves.")
    try:
        todas_las_claves = r.keys("*")
        log("6. RESULTADO", f"Total claves: {len(todas_las_claves)}", f"Claves: {todas_las_claves}")
    except Exception as e:
        log("6. ERROR", str(e))

# 7. Obtener y mostrar todos los valores guardados
def req_7_obtener_todos_los_valores(r):
    log("7. OBTENER TODOS LOS VALORES",
        "Usamos KEYS * y luego MGET (Multi-GET) para obtener los valores.")
    try:
        todas_las_claves = r.keys("*")
        if todas_las_claves:
            valores = r.mget(todas_las_claves)
            log("7. RESULTADO", f"Valores: {valores}")
        else:
            log("7. RESULTADO", "No hay claves para obtener valores.")
    except Exception as e:
        log("7. ERROR", str(e))

# 8. Obtener registros con patrón *
def req_8_patron_asterisco(r):
    log("8. OBTENER CLAVES CON PATRÓN * (Comodín)",
        "El * sustituye cero o más caracteres.",
        "Ej: KEYS tienda:producto:1001:* (Obtiene nombre y stock)")
    try:
        patron = "tienda:producto:1001:*"
        claves = r.keys(patron)
        valores = r.mget(claves) if claves else []
        log("8. RESULTADO", f"Claves para '{patron}': {claves}", f"Valores: {valores}")
    except Exception as e:
        log("8. ERROR", str(e))

# 9. Obtener registros con patrón []
def req_9_patron_corchetes(r):
    log("9. OBTENER CLAVES CON PATRÓN [] (Rango)",
        "[] define un conjunto de caracteres.",
        "Ej: KEYS tienda:producto:100[1-2]:nombre (Nombre de 1001 y 1002)")
    try:
        patron = "tienda:producto:100[1-2]:nombre"
        claves = r.keys(patron)
        valores = r.mget(claves) if claves else []
        log("9. RESULTADO", f"Claves para '{patron}': {claves}", f"Valores: {valores}")
    except Exception as e:
        log("9. ERROR", str(e))

# 10. Obtener registros con patrón ?
def req_10_patron_interrogacion(r):
    log("10. OBTENER CLAVES CON PATRÓN ? (Un caracter)",
        "? sustituye exactamente un caracter.",
        "Ej: KEYS tienda:categoria:Bebidas:?")
    try:
        patron = "tienda:categoria:Bebidas:?"
        claves = r.keys(patron)
        valores = r.mget(claves) if claves else []
        log("10. RESULTADO", f"Claves para '{patron}': {claves}", f"Valores: {valores}")
    except Exception as e:
        log("10. ERROR", str(e))

# 11. Filtrar registros por un valor (Modo Básico)
def req_11_filtrar_por_valor(r):
    log("11. FILTRAR POR VALOR (MODO BÁSICO)",
        "No se puede filtrar por valor directamente en claves string.",
        "Obtenemos claves, luego valores (MGET), y filtramos en Python.")
    try:
        claves_stocks = r.keys("tienda:producto:*:stock")
        stocks = r.mget(claves_stocks)
        
        # Combinamos claves y stocks, y filtramos
        productos_filtrados = []
        for i, clave in enumerate(claves_stocks):
            stock_val = int(stocks[i])
            if stock_val < 100: # Filtro: stock < 100
                # Obtenemos el nombre del producto asociado
                clave_nombre = clave.replace(":stock", ":nombre")
                nombre = r.get(clave_nombre)
                productos_filtrados.append(f"{nombre} (Stock: {stock_val})")
                
        log("11. RESULTADO", f"Productos con stock < 100: {productos_filtrados}")
    except Exception as e:
        log("11. ERROR", str(e))

# 12. Actualizar registros por filtro (Modo Básico)
def req_12_actualizar_por_filtro(r):
    log("12. ACTUALIZAR POR FILTRO (MODO BÁSICO)",
        "Aumentar en 10 el stock de todos los productos (claves '...:stock')")
    try:
        claves_stocks = r.keys("tienda:producto:*:stock")
        log("12. INFO", f"Claves a actualizar: {claves_stocks}")
        
        # Usamos INCRBY para aumentar atómicamente
        actualizados = []
        for clave in claves_stocks:
            nuevo_valor = r.incrby(clave, 10)
            actualizados.append(f"{clave} -> {nuevo_valor}")
            
        log("12. RESULTADO", f"Stocks actualizados (+10): {actualizados}")
    except Exception as e:
        log("12. ERROR", str(e))

# 13. Eliminar registros por filtro (Modo Básico)
def req_13_eliminar_por_filtro(r):
    log("13. ELIMINAR POR FILTRO (MODO BÁSICO)",
        "Eliminar todas las claves de la categoría 'Bebidas'.",
        "Ej: KEYS tienda:categoria:Bebidas:*")
    try:
        claves_a_eliminar = r.keys("tienda:categoria:Bebidas:*")
        if claves_a_eliminar:
            r.delete(*claves_a_eliminar) # El * desempaqueta la lista
            log("13. RESULTADO", f"Claves eliminadas: {claves_a_eliminar}")
        else:
            log("13. RESULTADO", "No se encontraron claves para eliminar.")
    except Exception as e:
        log("13. ERROR", str(e))

# 14. Crear estructura JSON
def req_14_crear_json(r):
    log("14. CREAR ESTRUCTURA JSON (RedisJSON)",
        "Almacenamos datos de productos como documentos JSON.",
        "Esto requiere el módulo RedisJSON (incluido en redis-stack).",
        "Usamos JSON.SET <clave> $ <datos_json>")
    
    # Datos de la empresa (Tienda de productos)
    productos = {
        "prod:1001": {
            "id_producto": "prod:1001", "nombre": "Café Grano Colombia",
            "categoria": "Bebidas", "stock": 150, "precio": 12.99
        },
        "prod:1002": {
            "id_producto": "prod:1002", "nombre": "Té Verde Matcha",
            "categoria": "Bebidas", "stock": 90, "precio": 25.50
        },
        "prod:1003": {
            "id_producto": "prod:1003", "nombre": "Croissant Almendra",
            "categoria": "Panaderia", "stock": 50, "precio": 2.75
        },
        "prod:1004": {
            "id_producto": "prod:1004", "nombre": "Pan Integral Semillas",
            "categoria": "Panaderia", "stock": 40, "precio": 4.50
        }
    }
    
    try:
        pipeline = r.pipeline()
        for p_id, data in productos.items():
            # JSON.SET clave ruta_json datos_json
            pipeline.json().set(f"producto:{p_id}", "$", data)
        pipeline.execute()
        
        # Verificamos uno
        prod_1001 = r.json().get("producto:prod:1001")
        log("14. RESULTADO",
            f"{len(productos)} documentos JSON creados.",
            f"Ejemplo (prod:1001): {prod_1001}")
    except Exception as e:
        log("14. ERROR", str(e))

# 15. Filtrar por atributo JSON (Requiere Índice - Ver Req 18)
def req_15_filtrar_json(r):
    log("15. FILTRAR JSON POR ATRIBUTO (RedisSearch)",
        "Esta función depende del índice creado en el paso 18.",
        "Filtro 1: Buscar productos de categoría 'Bebidas' (TAG)",
        "Filtro 2: Buscar productos con stock > 100 (NUMERIC)",
        "Filtro 3: Buscar 'Pan' en el nombre (TEXT)")
    try:
        # F1: Por TAG (Categoría)
        q1 = Query("@categoria:{Bebidas}")
        res1 = r.ft("idx:productos").search(q1)
        log("15. RESULTADO (Filtro 1: @categoria:{Bebidas})",
            f"Encontrados: {res1.total}",
            f"Docs: {[doc.json for doc in res1.docs]}")
        
        # F2: Por NUMERIC (Stock)
        q2 = Query("@stock:[ (100 +inf ]") # (100 -> exclusivo, +inf -> infinito
        res2 = r.ft("idx:productos").search(q2)
        log("15. RESULTADO (Filtro 2: @stock > 100)",
            f"Encontrados: {res2.total}",
            f"Docs: {[doc.json for doc in res2.docs]}")

        # F3: Por TEXT (Nombre)
        q3 = Query("@nombre:Pan")
        res3 = r.ft("idx:productos").search(q3)
        log("15. RESULTADO (Filtro 3: @nombre:Pan)",
            f"Encontrados: {res3.total}",
            f"Docs: {[doc.json for doc in res3.docs]}")

    except Exception as e:
        log("15. ERROR", f"¿El índice 'idx:productos' existe? (Se crea en paso 18). {e}")

# 16. Crear una lista
def req_16_crear_lista(r):
    log("16. CREAR UNA LISTA (Listas)",
        "Usamos LPUSH para añadir elementos al inicio de una lista.",
        "Ej: LPUSH pedidos:usuario:501 'prod:1001'")
    try:
        r.lpush("pedidos:usuario:501", "prod:1001") # Pedido 1
        r.lpush("pedidos:usuario:501", "prod:1003") # Pedido 2
        r.lpush("pedidos:usuario:501", "prod:1001") # Pidió otro café
        
        longitud = r.llen("pedidos:usuario:501")
        log("16. RESULTADO", f"Lista 'pedidos:usuario:501' creada con {longitud} elementos.")
    except Exception as e:
        log("16. ERROR", str(e))

# 17. Obtener elementos de lista con filtro
def req_17_filtrar_lista(r):
    log("17. OBTENER ELEMENTOS DE LISTA",
        "Usamos LRANGE para obtener un rango de elementos.",
        "Ej: LRANGE pedidos:usuario:501 0 -1 (Obtiene toda la lista)")
    try:
        # Filtro: Obtener todos los elementos
        items = r.lrange("pedidos:usuario:501", 0, -1)
        log("17. RESULTADO (Todos los elementos)", f"Items: {items}")
        
        # Filtro: Obtener los 2 más recientes (índices 0 y 1)
        items_recientes = r.lrange("pedidos:usuario:501", 0, 1)
        log("17. RESULTADO (2 más recientes)", f"Items: {items_recientes}")
    except Exception as e:
        log("17. ERROR", str(e))

# 18. Crear datos con índices (RedisSearch)
def req_18_crear_indices(r):
    log("18. CREAR ÍNDICE (RedisSearch)",
        "Definimos un esquema sobre los datos JSON (Req 14).",
        "Esto permite búsquedas complejas (Req 15, 19, 20).")
    
    # Definición del esquema
    schema = (
        TextField("$.nombre", as_name="nombre", sortable=True),
        TagField("$.categoria", as_name="categoria", separator=','),
        NumericField("$.stock", as_name="stock", sortable=True),
        NumericField("$.precio", as_name="precio", sortable=True)
    )
    
    # Definición del índice
    idx_def = IndexDefinition(
        prefix=["producto:"],           # Indexar claves que empiecen con 'producto:'
        index_type=IndexType.JSON       # Indexar documentos JSON
    )
    
    try:
        # Borrar índice si ya existe (para pruebas)
        try:
            r.ft("idx:productos").dropindex(delete_documents=False)
            log("18. INFO", "Índice 'idx:productos' anterior eliminado.")
        except redis.exceptions.ResponseError:
            log("18. INFO", "Índice 'idx:productos' no existía. Se creará.")

        # Crear el índice
        r.ft("idx:productos").create_index(fields=schema, definition=idx_def)
        log("18. RESULTADO", "Índice 'idx:productos' creado con éxito.")
    except Exception as e:
        log("18. ERROR", str(e))

# 19. Realizar búsqueda con índices (por campo)
def req_19_busqueda_indices(r):
    log("19. BÚSQUEDA CON ÍNDICES (POR CAMPO)",
        "Buscamos productos con precio entre 3 y 15.",
        "Ej: @precio:[3 15]")
    try:
        q = Query("@precio:[3 15]")
        res = r.ft("idx:productos").search(q)
        
        log("19. RESULTADO (Productos con 3 <= precio <= 15)",
            f"Encontrados: {res.total}",
            f"Docs: {[doc.json for doc in res.docs]}")
    except Exception as e:
        log("19. ERROR", str(e))

# 20. Realizar GROUP BY usando índices
def req_20_group_by_indices(r):
    log("20. GROUP BY CON ÍNDICES (AGGREGATE)",
        "Agregamos datos para contar cuántos productos hay por categoría.",
        "También calculamos el stock promedio por categoría.")
    try:
        # Petición de agregación
        req = AggregationRequest("*").\
                group_by("@categoria",  # Agrupar por categoría
                         # Contar productos en el grupo
                         "@count(0)", 
                         # Calcular media de stock
                         "@avg($.stock)", "AS", "stock_promedio" 
                         ).\
                sort_by(DescSorting("@count")) # Ordenar por conteo
        
        res = r.ft("idx:productos").aggregate(req)
        
        # Formatear resultado
        resultados_agg = []
        for row in res.rows:
            resultados_agg.append(
                f"Categoría: {row[row.index('categoria')]}, "
                f"Conteo: {row[row.index('count')]}, "
                f"Stock Promedio: {float(row[row.index('stock_promedio')]):.2f}"
            )
            
        log("20. RESULTADO (Conteo y Stock por Categoría)",
            f"Grupos encontrados: {len(res.rows)}",
            f"Resultados: {resultados_agg}")
            
    except Exception as e:
        log("20. ERROR", str(e))

# --- Ejecución Principal ---
def main():
    try:
        r = redis.Redis(host=REDIS_HOST, port=6379, decode_responses=True)
        r.ping()
        log("Conexión a Redis exitosa", f"Host: {REDIS_HOST}:6379")
        
        # Limpiar BD al inicio
        limpiar_db(r)
        
        # Ejecutar Requisitos 1-13 (K-V Básico)
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
        
        # Ejecutar Requisitos 14-20 (JSON, Listas, Índices)
        # Limpiamos K-V básicos para centrarnos en JSON
        limpiar_db(r) 
        
        req_14_crear_json(r)
        req_16_crear_lista(r)
        req_17_filtrar_lista(r)
        
        # 18 debe ir antes de 15, 19, 20
        req_18_crear_indices(r) 
        
        # Ahora ejecutamos las búsquedas
        req_15_filtrar_json(r)
        req_19_busqueda_indices(r)
        req_20_group_by_indices(r)
        
        log("EJECUCIÓN COMPLETADA", "Todas las funciones (1-20) se han ejecutado.")

    except redis.exceptions.ConnectionError as e:
        log("ERROR DE CONEXIÓN", 
            f"No se pudo conectar a Redis en {REDIS_HOST}:6379.",
            "Asegúrate de que el contenedor de Docker 'redis_stack' esté corriendo.",
            f"Error: {e}")
    except Exception as e:
        log("ERROR INESPERADO", str(e))

if __name__ == "__main__":
    main()