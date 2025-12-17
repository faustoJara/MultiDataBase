import pymongo
import random
import json
from faker import Faker
from bson import ObjectId
from urllib.parse import quote_plus
import os 
# 1. Configuración y Conexion
# ---------------------------------------------------------
usuario=os.getenv("USUARIO_MONGO")
passw=os.getenv("PASSWORD_MONGO")

user = quote_plus(usuario)
password= quote_plus(passw)
MONGO_URI = f"mongodb+srv://{user}:{password}@cluster0.lgcffpl.mongodb.net/?appName=Cluster0"

try:
    client = pymongo.MongoClient(MONGO_URI)
    db = client["TV_StreamDB"]
    collection = db["series"]
    
    # Limpiamos la colección antes de empezar para no duplicar datos si corres el script varias veces
    collection.delete_many({})
    print("✅ Conexión exitosa y colección limpia.")

except Exception as e:
    print(f"❌ Error de conexión: {e}")
    exit()

# Inicializamos Faker
fake = Faker()

# Listas auxiliares para dar realismo
PLATAFORMAS = ["Netflix", "HBO Max", "Disney+", "Amazon Prime", "Apple TV+"]
GENEROS = ["Sci-Fi", "Drama", "Comedia", "Acción", "Terror", "Documental", "Thriller"]

# 2. Generación de Datos
# ---------------------------------------------------------

def generar_serie_base():
    """Genera un diccionario con la estructura base."""
    return {
        "titulo": fake.sentence(nb_words=3).replace(".", ""), # Títulos estilo lorem ipsum
        "plataforma": random.choice(PLATAFORMAS),
        "temporadas": random.randint(1, 15),
        # Seleccionamos entre 1 y 3 géneros al azar
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



# B) Insertar 10 series con datos faltantes
print("🔄 Generando 10 series incompletas...")
campos_posibles = ["puntuacion", "año_estreno", "temporadas", "genero"]

for _ in range(10):
    serie = generar_serie_base()
    # Eliminamos un campo aleatorio para cumplir el requerimiento
    campo_a_borrar = random.choice(campos_posibles)
    del serie[campo_a_borrar]
    series_data.append(serie)

# Inserción masiva (Bulk insert es más eficiente)
if series_data:
    collection.insert_many(series_data)
    print(f"✅ Se han insertado {len(series_data)} documentos en total.")

# 3. Consultas
# ---------------------------------------------------------

# A. Maratones Largas: > 5 temporadas y puntuación > 8.0
query_maratones = {
    "temporadas": {"$gt": 5},
    "puntuacion": {"$gt": 8.0}
}

# B. Joyas Recientes de Comedia: Género "Comedia" y año >= 2020
query_comedias = {
    "genero": "Comedia",  # MongoDB busca automáticamente dentro del array
    "año_estreno": {"$gte": 2020}
}

# C. Contenido Finalizado: finalizada es True
query_finalizadas = {
    "finalizada": True
}

# D. Inventada: "Originales de Netflix Aclamados"
# Series de Netflix con puntuación perfecta o casi perfecta (>= 9.0)
query_netflix_top = {
    "plataforma": "Netflix",
    "puntuacion": {"$gte": 9.0}
}

# 4. Exportación y Limpieza
# ---------------------------------------------------------

def exportar_a_json(query, nombre_archivo, descripcion):
    """
    Ejecuta la query, limpia el _id y guarda en JSON.
    """
    # Ejecutamos la consulta
    cursor = collection.find(query)
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

print("\n--- Iniciando Exportación ---")

exportar_a_json(query_maratones, "maratones.json", "Maratones Largas")
exportar_a_json(query_comedias, "comedias_recientes.json", "Joyas de Comedia")
exportar_a_json(query_finalizadas, "series_finalizadas.json", "Series Finalizadas")
exportar_a_json(query_netflix_top, "netflix_top.json", "Top Netflix (Inventada)")

print("\n✅ Proceso finalizado con éxito.")
