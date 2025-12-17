import pymongo
import random
import json
import os 
from faker import Faker
from bson.objectid import ObjectId # Importación correcta de ObjectId
from urllib.parse import quote_plus
from dotenv import load_dotenv # Herramienta para leer .env

# =========================================================
# 1. Configuración y Conexion
# =========================================================

# Cargar las variables de entorno
load_dotenv() 
usuario = os.getenv("USUARIO_MONGO")
passw = os.getenv("PASSWORD_MONGO")

if not usuario or not passw:
    print("❌ Error: MONGO_USER o MONGO_PASS no encontrados en el archivo .env")
    exit()

# Codificación de credenciales para la URI
user_encoded = quote_plus(usuario)
pass_encoded = quote_plus(passw)

MONGO_URI = f"mongodb+srv://{user_encoded}:{pass_encoded}@cluster0.lgcffpl.mongodb.net/?appName=Cluster0"

try:
    client = pymongo.MongoClient(MONGO_URI)
    db = client["TV_StreamDB"]
    
    # Colección principal
    collection_series = db["series"]
    
    # Nueva colección
    collection_produccion = db["detalles_produccion"] 
    
    # Limpiamos ambas colecciones antes de empezar
    collection_series.delete_many({})
    collection_produccion.delete_many({})
    print("✅ Conexión exitosa y colecciones (series, detalles_produccion) limpias.")

except Exception as e:
    print(f"❌ Error de conexión: {e}")
    exit()

# Inicializamos Faker y Listas auxiliares
fake = Faker()
PLATAFORMAS = ["Netflix", "HBO Max", "Disney+", "Amazon Prime", "Apple TV+"]
GENEROS = ["Sci-Fi", "Drama", "Comedia", "Acción", "Terror", "Documental", "Thriller"]
PAISES = ["EE.UU.", "Reino Unido", "Corea del Sur", "España", "Alemania", "Japón", "Canadá"]
# Nombres de ejemplo para el reparto
NOMBRES_ACTORES = [fake.name() for _ in range(50)]

# =========================================================
# 2. Generación de Datos
# =========================================================

def generar_serie_base():
    """Genera un diccionario con la estructura base."""
    # Usamos un título único y predecible para el ejemplo
    return {
        "titulo": fake.unique.catch_phrase().replace(".", ""), 
        "plataforma": random.choice(PLATAFORMAS),
        "temporadas": random.randint(1, 15),
        "genero": random.sample(GENEROS, k=random.randint(1, 3)), 
        "puntuacion": round(random.uniform(5.0, 10.0), 1),
        "finalizada": random.choice([True, False]),
        "año_estreno": random.randint(2000, 2024)
    }

series_data = []

# A) Insertar 50 series completas
print("🔄 Generando 50 series completas...")
for _ in range(50):
    series_data.append(generar_serie_base())

# B) Insertar 10 series con datos faltantes (específicamente la puntuación)
print("🔄 Generando 10 series incompletas (algunas sin puntuación)...")
campos_posibles = ["puntuacion", "año_estreno", "temporadas", "genero"]

for _ in range(10):
    serie = generar_serie_base()
    # Aseguramos que al menos 5 series no tengan "puntuacion" para probar el punto 5
    if _ < 5:
        del serie["puntuacion"]
    else:
        campo_a_borrar = random.choice(campos_posibles)
        del serie[campo_a_borrar]
        
    series_data.append(serie)

# Inserción masiva
if series_data:
    collection_series.insert_many(series_data)
    print(f"✅ Se han insertado {len(series_data)} documentos en 'series' en total.")

# =========================================================
# 5. Media de Puntuación (Aggregations)
# =========================================================

print("\n--- 5. Obtener media de puntuación ---")

# Pipeline de agregación para calcular la media, excluyendo documentos sin el campo 'puntuacion'
pipeline_media = [
    # 1. Sólo considera documentos que tengan el campo 'puntuacion'
    {"$match": {"puntuacion": {"$exists": True}}}, 
    # 2. Agrupa todos los documentos restantes (id: null) y calcula la media
    {"$group": {"_id": None, "media_puntuacion": {"$avg": "$puntuacion"}}} 
]

try:
    resultado_media = list(collection_series.aggregate(pipeline_media))
    
    if resultado_media:
        media = resultado_media[0]["media_puntuacion"]
        print(f"📊 La media de puntuación de todas las series (con puntuación registrada) es: {media:.2f}")
    else:
        print("⚠️ No se encontraron series con el campo 'puntuacion' para calcular la media.")

except Exception as e:
    print(f"❌ Error al calcular la media: {e}")


# =========================================================
# 6. Unificar con otra colección (Creación y Lookup)
# =========================================================

print("\n--- 6. Unificar con otra colección ---")

# 6.1 Crear e insertar documentos en detalles_produccion

# 1. Obtener todos los títulos de la colección series
titulos_series = collection_series.find({}, {"_id": 0, "titulo": 1})
titulos_list = [doc["titulo"] for doc in titulos_series]

detalles_produccion_data = []

for titulo in titulos_list:
    detalles = {
        "titulo": titulo, # Campo de enlace (join key)
        "pais_origen": random.choice(PAISES),
        "reparto_principal": random.sample(NOMBRES_ACTORES, k=random.randint(3, 5)),
        # Presupuesto entre 1 y 20 millones
        "presupuesto_por_episodio": round(random.uniform(1.0, 20.0), 2)
    }
    detalles_produccion_data.append(detalles)

if detalles_produccion_data:
    collection_produccion.insert_many(detalles_produccion_data)
    print(f"✅ Se han insertado {len(detalles_produccion_data)} documentos en 'detalles_produccion'.")


# 6.2 Consulta con $lookup (Series Finalizadas, Puntuación > 8.0 y de EE.UU.)

print("\n🔄 Realizando consulta de unión (Finalizadas, >8.0, EE.UU.)...")

pipeline_join = [
    # 1. $match en la colección series (Series Finalizadas y Puntuación > 8.0)
    {"$match": {
        "finalizada": True,
        "puntuacion": {"$gt": 8.0}
    }},
    # 2. $lookup: Unir con detalles_produccion
    {"$lookup": {
        "from": "detalles_produccion", # Nombre de la colección a unir
        "localField": "titulo",         # Campo en la colección actual (series)
        "foreignField": "titulo",       # Campo en la colección unida (detalles_produccion)
        "as": "detalles"                # Nombre del array donde se almacenará el resultado
    }},
    # 3. $unwind: Descomponer el array 'detalles' (asumiendo que hay 1 a 1 por título)
    {"$unwind": "$detalles"},
    # 4. $match para filtrar por el campo de la colección unida (país_origen)
    {"$match": {
        "detalles.pais_origen": "EE.UU."
    }},
    # 5. $project: Limpiar y seleccionar los campos a mostrar
    {"$project": {
        "_id": 0,
        "titulo": 1,
        "plataforma": 1,
        "puntuacion": 1,
        "pais_origen": "$detalles.pais_origen",
        "presupuesto": "$detalles.presupuesto_por_episodio"
    }}
]

resultados_join = list(collection_series.aggregate(pipeline_join))

if resultados_join:
    print(f"✅ Se encontraron {len(resultados_join)} series que cumplen los criterios:")
    for doc in resultados_join:
        print(f"  - Título: {doc['titulo']} | Plataforma: {doc['plataforma']} | Puntuación: {doc['puntuacion']} | País: {doc['pais_origen']}")
else:
    print("⚠️ No se encontraron series que cumplan el criterio (Finalizadas >8.0 y de EE.UU.).")

# =========================================================
# 3. Consultas (Puntos A, B, C, D originales)
# =========================================================

# (Se mantienen los puntos 3 y 4 originales, usando collection_series)
# ... (Tu código para consultas y exportación aquí)

# =========================================================
# 4. Exportación y Limpieza
# =========================================================

def exportar_a_json(query, nombre_archivo, descripcion):
    """
    Ejecuta la query, limpia el _id y guarda en JSON.
    """
    # Ejecutamos la consulta
    cursor = collection_series.find(query)
    resultados = list(cursor)
    
    # Limpieza: Convertir ObjectId a string
    for doc in resultados:
        if '_id' in doc:
            doc['_id'] = str(doc['_id'])
    
    # Guardar archivo
    try:
        with open(nombre_archivo, 'w', encoding='utf-8') as f:
            json.dump(resultados, f, indent=4, ensure_ascii=False)
        print(f"📁 {descripcion}: {len(resultados)} registros exportados a '{nombre_archivo}'")
    except IOError as e:
        print(f"❌ Error al guardar {nombre_archivo}: {e}")

# A. Maratones Largas: > 5 temporadas y puntuación > 8.0
query_maratones = {
    "temporadas": {"$gt": 5},
    "puntuacion": {"$gt": 8.0}
}

# B. Joyas Recientes de Comedia: Género "Comedia" y año >= 2020
query_comedias = {
    "genero": "Comedia",
    "año_estreno": {"$gte": 2020}
}

# C. Contenido Finalizado: finalizada es True
query_finalizadas = {
    "finalizada": True
}

# D. Inventada: "Originales de Netflix Aclamados"
query_netflix_top = {
    "plataforma": "Netflix",
    "puntuacion": {"$gte": 9.0}
}

print("\n--- Iniciando Exportación (Puntos originales) ---")

exportar_a_json(query_maratones, "maratones.json", "Maratones Largas")
exportar_a_json(query_comedias, "comedias_recientes.json", "Joyas de Comedia")
exportar_a_json(query_finalizadas, "series_finalizadas.json", "Series Finalizadas")
exportar_a_json(query_netflix_top, "netflix_top.json", "Top Netflix (Inventada)")

# =========================================================
# 7. Gasto Financiero (Cálculo y Exportación)
# =========================================================

print("\n--- 7. Cálculo del Gasto Financiero Total ---")

# Constante de episodios por temporada
EPISODIOS_POR_TEMPORADA = 8

pipeline_costo_total = [
    # 1. $lookup: Unir series con detalles_produccion (Usando 'titulo')
    {"$lookup": {
        "from": "detalles_produccion", 
        "localField": "titulo",         
        "foreignField": "titulo",       
        "as": "produccion"                
    }},
    # 2. $unwind: Descomponer el array de 'produccion'
    {"$unwind": "$produccion"},
    
    # 3. $project: Calcular el costo total y seleccionar los campos a mostrar
    {"$project": {
        "_id": 0,
        "titulo": 1,
        # Cálculo: (temporadas * 8) * presupuesto_por_episodio
        "coste_total": {
            "$multiply": [
                "$produccion.presupuesto_por_episodio", 
                {"$multiply": ["$temporadas", EPISODIOS_POR_TEMPORADA]}
            ]
        },
        # Incluimos estos campos para verificación, aunque solo pediste título y coste
        "temporadas": 1,
        "presupuesto_episodio": "$produccion.presupuesto_por_episodio"
    }},
    # 4. $project (Opcional): Simplificar la salida final a solo título y coste total
    {"$project": {
        "titulo": 1,
        "coste_total_millones": {"$round": ["$coste_total", 2]} # Redondeamos a 2 decimales
    }}
]

resultados_costo = list(collection_series.aggregate(pipeline_costo_total))

def exportar_costo_a_json(data, nombre_archivo, descripcion):
    """
    Guarda los resultados de agregación en un JSON.
    """
    try:
        with open(nombre_archivo, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=4, ensure_ascii=False)
        print(f"📁 {descripcion}: {len(data)} registros exportados a '{nombre_archivo}'")
    except IOError as e:
        print(f"❌ Error al guardar {nombre_archivo}: {e}")

exportar_costo_a_json(resultados_costo, "gasto_financiero.json", "Costo Total de las Series")

print("\n✅ Cálculo del gasto financiero finalizado y exportado.")