import boto3
import mysql.connector
import json
import os
from boto3.dynamodb.conditions import Attr
from decimal import Decimal
from dotenv import load_dotenv

load_dotenv()

REGION = os.getenv("AWS_REGION", "us-east-1")
RDS_HOST = os.getenv("RDS_HOST")
RDS_USER = os.getenv("RDS_USER")
RDS_PASS = os.getenv("RDS_PASS")
DB_NAME = os.getenv("RDS_DB_NAME", "CentroCuidadoNube")

# Clase auxiliar para convertir Decimal de DynamoDB a float
class DecimalEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            return float(obj)
        return super(DecimalEncoder, self).default(obj)

def obtener_datos_dynamo():
    print(" -> Consultando DynamoDB (Usuarios Activos)...")
    try:
        # Boto3 leerá automáticamente las credenciales cargadas por load_dotenv()
        dynamodb = boto3.resource('dynamodb', region_name=REGION)
        table = dynamodb.Table('CentroCuidado_Usuarios_GSI')
        response = table.scan(FilterExpression=Attr('Estado').eq('Activo'))
        return response.get('Items', [])
    except Exception as e:
        print(f"Error DynamoDB: {e}")
        return []

def obtener_datos_rds():
    print(" -> Consultando RDS (Reportes)...")
    try:
        conn = mysql.connector.connect(
            host=RDS_HOST, user=RDS_USER, password=RDS_PASS, database=DB_NAME
        )
        cursor = conn.cursor(dictionary=True) 
        cursor.execute("SELECT * FROM ReportesNube")
        resultados = cursor.fetchall()
        for r in resultados:
            if 'Fecha' in r:
                r['Fecha'] = str(r['Fecha'])
        conn.close()
        return resultados
    except Exception as e:
        print(f"Error RDS: {e}")
        return []

def generar_json_unificado():
    print("\n--- INICIANDO INTEGRACIÓN DE DATOS (AWS) ---")
    
    if not RDS_HOST:
        print("❌ Error: Faltan variables de entorno RDS en el archivo .env")
        return

    datos_dynamo = obtener_datos_dynamo()
    datos_rds = obtener_datos_rds()
    
    informe_final = {
        "origen": "AWS Lab Integration",
        "estadisticas": {
            "total_usuarios_activos_dynamo": len(datos_dynamo),
            "total_reportes_rds": len(datos_rds)
        },
        "datos": {
            "usuarios_cloud": datos_dynamo,
            "reportes_sistema": datos_rds
        }
    }
    
    nombre_archivo = "aws_integration_result.json"
    
    with open(nombre_archivo, 'w', encoding='utf-8') as f:
        json.dump(informe_final, f, indent=4, ensure_ascii=False, cls=DecimalEncoder)
        
    print(f"\n✅ Archivo generado con éxito: {nombre_archivo}")
    print("Contenido parcial:")
    print(json.dumps(informe_final, indent=2, ensure_ascii=False, cls=DecimalEncoder)[:500] + "...")

if __name__ == "__main__":
    generar_json_unificado()