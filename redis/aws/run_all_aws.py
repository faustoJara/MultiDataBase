import sys
import os
import subprocess

# Añadimos la carpeta actual al path para poder importar los managers
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

try:
    # Intentamos importar los managers.
    import dynamo_manager
    import rds_manager
    import data_integration
    from dotenv import load_dotenv
    # Cargamos el .env para los scripts
    load_dotenv()
except ImportError as e:
    print("\n[ERROR CRÍTICO] Falta un módulo: Asegúrate de que las librerías de AWS")
    print("estén instaladas en tu entorno local.")
    print("Comando recomendado: pip install -r aws/requirements.txt")
    sys.exit(1)

def main_flow():
    """Ejecuta toda la funcionalidad de gestión de AWS y la integración en secuencia."""
    
    print("\n==================================================================")
    print("  INICIO DEL FLUJO COMPLETO: GESTIÓN E INTEGRACIÓN AWS EN LA NUBE")
    print("==================================================================")

    # 1. GESTIÓN DE RDS (Crea DB, Tablas, Consultas SQL)
    try:
        print("\n--- PASO 1: GESTIÓN Y POBLACIÓN DE RDS (MySQL Nube) ---")
        rds_manager.gestionar_rds()
        print("\n[ÉXITO] Funcionalidad RDS completada.")
    except Exception as e:
        print(f"\n[ERROR FATAL] Fallo en RDS Manager: {e}")
        return

    # 2. GESTIÓN DE DYNAMODB (Crea Tablas, Inserta, CRUD, PartiQL)
    try:
        print("\n--- PASO 2: GESTIÓN Y POBLACIÓN DE DYNAMODB (NoSQL Nube) ---")
        dynamo_manager.main()
        print("\n[ÉXITO] Funcionalidad DynamoDB completada.")
    except Exception as e:
        print(f"\n[ERROR FATAL] Fallo en DynamoDB Manager: {e}")
        return

    # 3. INTEGRACIÓN (DynamoDB + RDS -> JSON)
    try:
        print("\n--- PASO 3: INTEGRACIÓN Y GENERACIÓN DE INFORME JSON ---")
        data_integration.generar_json_unificado()
        print("\n[ÉXITO] Integración de datos completada.")
    except Exception as e:
        print(f"\n[ERROR FATAL] Fallo en Integración JSON: {e}")
        return

if __name__ == "__main__":
    main_flow()