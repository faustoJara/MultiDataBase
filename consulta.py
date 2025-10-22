
import mysql.connector
import json
from datetime import datetime
from decimal import Decimal

# --- CONFIGURACIÓN DE CONEXIÓN ---
DB_NAME = "CENTROCUIDADOFAMILIAR"
DB_CONFIG = {
    'user': 'root',
    'password': 'mi_clave',      
    'host': '127.0.0.1',
    'port': 3306,               
    'database': DB_NAME
}

OUTPUT_FILE = "datos_analisis.json"

def get_data_for_analysis():
    """ Obtiene datos combinados de USUARIA, SERVICIO y RESENA. """
    print("-> Conectando a MySQL y obteniendo datos para análisis de discriminación...")
    conn = None
    
    # Esta consulta cruza las variables demograficas sensibles con la calidad del servicio (Puntuación)
    QUERY = """
    SELECT
        U.IDUsuario,
        U.Barrio,
        U.RentaPercapita,
        U.Genero,
        S.PrecioFinal,
        S.SubvencionAplicada,
        R.Puntuacion,
        D.TipoDependencia
    FROM
        USUARIA U
    JOIN
        SERVICIO S ON U.IDUsuario = S.IDUsuario
    JOIN
        DEPENDIENTE D ON S.IDDependiente = D.IDDependiente
    LEFT JOIN
        RESENA R ON S.IDServicio = R.IDServicio
    WHERE
        S.Estado = 'Completado' AND R.Puntuacion IS NOT NULL
    """
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor(dictionary=True)
        cursor.execute(QUERY)
        data = cursor.fetchall()
        print(f"  Se obtuvieron {len(data)} registros de servicios completados y reseñados.")
        return data

    except mysql.connector.Error as err:
        print(f"  MySQL ERROR: {err.msg}")
        return None
        
    finally:
        if conn and conn.is_connected():
            cursor.close()
            conn.close()


def generate_analysis_json(raw_data):
    """ Procesa los datos y genera un archivo JSON con análisis y datos discriminantes. """
    if not raw_data:
        print("  No se pudo generar el JSON por falta de datos.")
        return

    # --- 1. PRE-PROCESAMIENTO Y AGREGACIÓN ---
    processed_data = []
    
    # Diccionarios para análisis: {Clave: [Puntuaciones], ...}
    gender_scores_map = {} 
    
    for row in raw_data:
        processed_row = dict(row) 
        
        # Conversión de tipos para JSON (Decimal a float)
        for key in ['RentaPercapita', 'PrecioFinal', 'SubvencionAplicada']:
            if processed_row.get(key) is not None:
                 # Usa float() para convertir Decimal a flotante (para JSON)
                processed_row[key] = float(processed_row[key]) 

        processed_data.append(processed_row)

        # Agregación por GÉNERO
        gender = row.get('Genero') or 'No especificado'
        score = row.get('Puntuacion')
        
        if score is not None:
            if gender not in gender_scores_map:
                gender_scores_map[gender] = []
            gender_scores_map[gender].append(score)


    # --- 2. ANÁLISIS AGREGADO (INDICADORES CLAVE) ---
    
    # Puntuación Promedio por Género
    avg_scores_gender = {
        gender: round(sum(scores) / len(scores), 2)
        for gender, scores in gender_scores_map.items() if scores
    }
    
    # Creación de Grupos de Renta (Baja: < 20k, Media: 20k-40k, Alta: > 40k)
    renta_scores_map = {'Baja': [], 'Media': [], 'Alta': []}
    for row in raw_data:
        renta = row.get('RentaPercapita')
        score = row.get('Puntuacion')

        if renta is not None and score is not None:
            if renta < 20000:
                renta_scores_map['Baja'].append(score)
            elif renta <= 40000:
                renta_scores_map['Media'].append(score)
            else:
                renta_scores_map['Alta'].append(score)

    avg_scores_renta = {
        grupo: round(sum(scores) / len(scores), 2)
        for grupo, scores in renta_scores_map.items() if scores
    }


    # --- 3. GENERACIÓN DEL JSON FINAL ---
    final_json = {
        "metadata": {
            "fecha_generacion": datetime.now().isoformat(),
            "descripcion": "Análisis discriminante que cruza variables socioeconómicas (Género, Renta) con la Calidad del Servicio (Puntuación) y el Costo (Precio/Subvención).",
            "registros_analizados_con_resena": len(processed_data)
        },
        "indicadores_clave_discriminacion": {
            "puntuacion_promedio_por_genero": avg_scores_gender,
            "puntuacion_promedio_por_grupo_renta": avg_scores_renta,
        },
        "datos_discriminantes_crudos": processed_data
    }

    # Guardar en archivo JSON
    try:
        with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
            json.dump(final_json, f, ensure_ascii=False, indent=4)
        print(f"\n Archivo JSON de análisis generado con éxito: {OUTPUT_FILE}")
    except Exception as e:
        print(f" ERROR al guardar el JSON: {e}")


if __name__ == '__main__':
    data = get_data_for_analysis()
    generate_analysis_json(data)