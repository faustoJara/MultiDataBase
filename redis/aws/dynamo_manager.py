import boto3
import time
import sys
import os # Necesario para leer variables de entorno
from boto3.dynamodb.conditions import Key, Attr
from botocore.exceptions import ClientError
from dotenv import load_dotenv # Necesario para leer el archivo .env

# === CONFIGURACIÓN ===
# --- CARGAR VARIABLES DE ENTORNO ---
load_dotenv()
REGION = os.getenv('AWS_REGION', 'us-east-1') # Cargar la región desde .env

try:
    # Boto3 leerá automáticamente las claves (ACCESS_KEY_ID, SECRET_KEY, SESSION_TOKEN)
    # desde las variables de entorno cargadas por load_dotenv().
    dynamodb = boto3.resource('dynamodb', region_name=REGION)
    dynamodb_client = boto3.client('dynamodb', region_name=REGION)
except Exception as e:
    print(f"Error conectando a AWS: {e}")
    print("Verifica tu archivo .env y asegúrate de que todas las claves AWS_ esten presentes.")
    sys.exit(1)

# Nombres de Tablas
T_SIMPLE = 'CentroCuidado_Log_Simple'      # Tabla 1: Sin índices secundarios
T_LSI = 'CentroCuidado_Pagos_LSI'          # Tabla 2: Con Índice Local (LSI)
T_GSI = 'CentroCuidado_Usuarios_GSI'       # Tabla 3: Con Índice Global (GSI)

def log(titulo, mensaje):
    print(f"\n{'='*60}\n  {titulo}\n{'='*60}")
    for line in mensaje.split('\n'):
        print(f" -> {line}")
    time.sleep(1)

# --- 1. CREACIÓN DE TABLAS ---

def crear_tablas():
    log("1. CREACIÓN DE TABLAS", "Verificando y creando tablas con Índices en DynamoDB...")
    try:
        existing_tables = dynamodb_client.list_tables()['TableNames']
    except ClientError as e:
        # Si falla aquí, es un error de permisos o región
        print(f"Error de credenciales o región: {e}")
        return

    # 1. Tabla Simple (Log de accesos) - PK: LogID
    if T_SIMPLE not in existing_tables:
        print(f"Creando tabla simple: {T_SIMPLE}...")
        dynamodb.create_table(
            TableName=T_SIMPLE,
            KeySchema=[{'AttributeName': 'LogID', 'KeyType': 'HASH'}], # Partition Key
            AttributeDefinitions=[{'AttributeName': 'LogID', 'AttributeType': 'S'}],
            ProvisionedThroughput={'ReadCapacityUnits': 5, 'WriteCapacityUnits': 5}
        )
    else:
        print(f"Tabla {T_SIMPLE} ya existe.")
    
    # 2. Tabla con LSI (Pagos) - PK: UsuarioID, SK: FechaPago. LSI: Monto
    if T_LSI not in existing_tables:
        print(f"Creando tabla con Índice Local (LSI): {T_LSI}...")
        dynamodb.create_table(
            TableName=T_LSI,
            KeySchema=[
                {'AttributeName': 'UsuarioID', 'KeyType': 'HASH'}, # PK
                {'AttributeName': 'FechaPago', 'KeyType': 'RANGE'} # SK
            ],
            AttributeDefinitions=[
                {'AttributeName': 'UsuarioID', 'AttributeType': 'S'},
                {'AttributeName': 'FechaPago', 'AttributeType': 'S'},
                {'AttributeName': 'Monto', 'AttributeType': 'N'} # Atributo para el índice
            ],
            LocalSecondaryIndexes=[{
                'IndexName': 'IndiceMonto',
                'KeySchema': [
                    {'AttributeName': 'UsuarioID', 'KeyType': 'HASH'},
                    {'AttributeName': 'Monto', 'KeyType': 'RANGE'} # Ordenar por monto
                ],
                'Projection': {'ProjectionType': 'ALL'}
            }],
            ProvisionedThroughput={'ReadCapacityUnits': 5, 'WriteCapacityUnits': 5}
        )
    else:
        print(f"Tabla {T_LSI} ya existe.")

    # 3. Tabla con GSI (Usuarios) - PK: UsuarioID. GSI: Email
    if T_GSI not in existing_tables:
        print(f"Creando tabla con Índice Global (GSI): {T_GSI}...")
        dynamodb.create_table(
            TableName=T_GSI,
            KeySchema=[{'AttributeName': 'UsuarioID', 'KeyType': 'HASH'}],
            AttributeDefinitions=[
                {'AttributeName': 'UsuarioID', 'AttributeType': 'S'},
                {'AttributeName': 'Email', 'AttributeType': 'S'} # Atributo para el índice global
            ],
            GlobalSecondaryIndexes=[{
                'IndexName': 'IndiceEmail',
                'KeySchema': [{'AttributeName': 'Email', 'KeyType': 'HASH'}], # PK del índice
                'Projection': {'ProjectionType': 'ALL'},
                'ProvisionedThroughput': {'ReadCapacityUnits': 5, 'WriteCapacityUnits': 5}
            }],
            ProvisionedThroughput={'ReadCapacityUnits': 5, 'WriteCapacityUnits': 5}
        )
    else:
        print(f"Tabla {T_GSI} ya existe.")
    
    # Es crucial esperar a que las tablas estén activas antes de PutItem
    print("Esperando 5 segundos...")
    time.sleep(5)
    # Usaremos wait_until_exists() en el futuro para mayor robustez, pero por ahora un sleep es suficiente si las tablas ya existen.


# --- 2. GESTIÓN DE DATOS ---

def crear_registros():
    log("2. INSERTAR DATOS", "Creando 3 registros en cada tabla...")
    
    # Tabla Simple
    t1 = dynamodb.Table(T_SIMPLE)
    t1.put_item(Item={'LogID': 'L1', 'Mensaje': 'Inicio sesión', 'Nivel': 'INFO'})
    t1.put_item(Item={'LogID': 'L2', 'Mensaje': 'Error conexión', 'Nivel': 'ERROR'})
    t1.put_item(Item={'LogID': 'L3', 'Mensaje': 'Logout', 'Nivel': 'INFO'})

    # Tabla LSI (Pagos)
    t2 = dynamodb.Table(T_LSI)
    t2.put_item(Item={'UsuarioID': 'U1', 'FechaPago': '2023-01-01', 'Monto': 100, 'Concepto': 'Mensualidad'})
    t2.put_item(Item={'UsuarioID': 'U1', 'FechaPago': '2023-02-01', 'Monto': 150, 'Concepto': 'Extra'})
    t2.put_item(Item={'UsuarioID': 'U2', 'FechaPago': '2023-01-05', 'Monto': 500, 'Concepto': 'Anual'})

    # Tabla GSI (Usuarios)
    t3 = dynamodb.Table(T_GSI)
    t3.put_item(Item={'UsuarioID': 'U100', 'Email': 'ana@test.com', 'Nombre': 'Ana', 'Estado': 'Activo'})
    t3.put_item(Item={'UsuarioID': 'U200', 'Email': 'luis@test.com', 'Nombre': 'Luis', 'Estado': 'Inactivo'})
    t3.put_item(Item={'UsuarioID': 'U300', 'Email': 'maria@test.com', 'Nombre': 'Maria', 'Estado': 'Activo'})
    
    print("Registros insertados correctamente.")

def operaciones_crud_basicas():
    log("3. CRUD BÁSICO", "Realizando Get, Update y Delete en las 3 tablas...")
    t1 = dynamodb.Table(T_SIMPLE)
    t2 = dynamodb.Table(T_LSI)
    t3 = dynamodb.Table(T_GSI)

    # --- T_SIMPLE (Log) ---
    resp = t1.get_item(Key={'LogID': 'L1'})
    print(f" -> T_SIMPLE: Obtener (L1): {resp.get('Item')}")
    t1.update_item(Key={'LogID': 'L1'}, UpdateExpression="set Mensaje=:m", ExpressionAttributeValues={':m': 'Inicio sesión (Actualizado)'})
    print(" -> T_SIMPLE: Registro L1 actualizado.")
    t1.delete_item(Key={'LogID': 'L3'})
    print(" -> T_SIMPLE: Registro L3 eliminado.")
    
    # --- T_LSI (Pagos) - PK: UsuarioID, SK: FechaPago ---
    resp = t2.get_item(Key={'UsuarioID': 'U1', 'FechaPago': '2023-01-01'})
    print(f" -> T_LSI: Obtener (U1/2023-01-01): {resp.get('Item')}")
    t2.update_item(
        Key={'UsuarioID': 'U1', 'FechaPago': '2023-01-01'},
        UpdateExpression="set Concepto=:c",
        ExpressionAttributeValues={':c': 'Mensualidad (Ajustada)'}
    )
    print(" -> T_LSI: Registro U1/2023-01-01 actualizado.")
    t2.delete_item(Key={'UsuarioID': 'U2', 'FechaPago': '2023-01-05'})
    print(" -> T_LSI: Registro U2/2023-01-05 eliminado.")

    # --- T_GSI (Usuarios) - PK: UsuarioID ---
    resp = t3.get_item(Key={'UsuarioID': 'U300'})
    print(f" -> T_GSI: Obtener (U300): {resp.get('Item')}")
    t3.update_item(
        Key={'UsuarioID': 'U300'},
        UpdateExpression="set Nombre=:n",
        ExpressionAttributeValues={':n': 'Maria Alejandra'}
    )
    print(" -> T_GSI: Registro U300 actualizado.")
    
    # Re-insertamos L3 en T_SIMPLE y U2/2023-01-05 en T_LSI para que las funciones posteriores (SCAN) no fallen por falta de datos
    t1.put_item(Item={'LogID': 'L3', 'Mensaje': 'Logout', 'Nivel': 'INFO'})
    t2.put_item(Item={'UsuarioID': 'U2', 'FechaPago': '2023-01-05', 'Monto': 500, 'Concepto': 'Anual'})
    print(" -> Datos eliminados reinsertados para pruebas posteriores.")


def obtener_todos_scan():
    log("4. SCAN (Obtener Todos)", "Obteniendo todos los registros de la tabla Simple.")
    t1 = dynamodb.Table(T_SIMPLE)
    resp = t1.scan()
    print(f" -> Registros encontrados: {resp['Items']}")

def filtrado_scan_gsi():
    log("5. SCAN CON FILTRO & GSI", "Filtrando datos en las tablas.")
    
    # SCAN FILTRADO
    t1 = dynamodb.Table(T_SIMPLE)
    resp = t1.scan(FilterExpression=Attr('Nivel').eq('ERROR'))
    print(f" -> Scan Filtrado (Nivel=ERROR): {resp['Items']}")

    # QUERY CON GSI (Índice Global)
    # Buscamos un usuario por Email, no por su ID. Esto solo es posible gracias al GSI.
    t3 = dynamodb.Table(T_GSI)
    resp_gsi = t3.query(
        IndexName='IndiceEmail',
        KeyConditionExpression=Key('Email').eq('luis@test.com')
    )
    print(f" -> Búsqueda por Índice Global (Email=luis@test.com): {resp_gsi['Items']}")

def eliminacion_condicional():
    log("6. ELIMINACIÓN CONDICIONAL", "Intentando borrar datos solo si cumplen condición.")
    t3 = dynamodb.Table(T_GSI)
    
    try:
        # Intentamos borrar U100 SOLO SI su Estado es 'Inactivo'. 
        print(" -> Intentando borrar Ana (Activo) con condición Estado=Inactivo...")
        t3.delete_item(
            Key={'UsuarioID': 'U100'},
            ConditionExpression=Attr('Estado').eq('Inactivo')
        )
    except ClientError as e:
        if e.response['Error']['Code'] == 'ConditionalCheckFailedException':
            print(" -> ¡Bloqueado por condición! (Correcto, no se borró).")
        else:
            print(f" -> Error inesperado: {e}")

    # Borramos Luis porque es Inactivo (Debe funcionar)
    t3.delete_item(
        Key={'UsuarioID': 'U200'},
        ConditionExpression=Attr('Estado').eq('Inactivo')
    )
    print(" -> Luis eliminado correctamente (cumplía condición).")

def filtros_complejos_lsi():
    log("7. FILTROS COMPLEJOS CON LSI", "Filtrando Pagos usando el índice local.")
    t2 = dynamodb.Table(T_LSI)
    
    # Usamos el índice local para buscar pagos de U1 mayores a 120
    resp = t2.query(
        IndexName='IndiceMonto',
        KeyConditionExpression=Key('UsuarioID').eq('U1') & Key('Monto').gt(120)
    )
    print(f" -> Pagos de U1 mayores a 120 (usando LSI): {resp['Items']}")

def uso_partiql():
    log("8. PARTIQL STATEMENT", "Usando sintaxis SQL para consultar DynamoDB en cada tabla.")
    
    # Generamos un ID único para la inserción en T_SIMPLE
    unique_partiql_id = f"PQL-{int(time.time())}" 
    
    # --- T_SIMPLE ---
    print(" -> T_SIMPLE (Select y Insert):")
    resp1 = dynamodb_client.execute_statement(
        Statement=f'SELECT * FROM "{T_SIMPLE}" WHERE Nivel = \'ERROR\''
    )
    print(f"   - Select ERROR: {resp1.get('Items')}")
    
    # INSERCIÓN CORREGIDA: Usamos el ID dinámico para evitar colisiones
    dynamodb_client.execute_statement(
        Statement=f"INSERT INTO \"{T_SIMPLE}\" VALUE {{'LogID': '{unique_partiql_id}', 'Mensaje': 'Creado con PartiQL', 'Nivel': 'WARN'}}"
    )
    print(f"   - Insertado registro {unique_partiql_id} con PartiQL.")
    
    # --- T_LSI ---
    print(" -> T_LSI (Select):")
    resp2 = dynamodb_client.execute_statement(
        Statement=f"SELECT Monto, Concepto FROM \"{T_LSI}\" WHERE UsuarioID = 'U1' AND FechaPago = '2023-02-01'"
    )
    print(f"   - Select U1/Feb: {resp2.get('Items')}")
    
    # --- T_GSI ---
    print(" -> T_GSI (Select y Update):")
    resp3 = dynamodb_client.execute_statement(
        Statement=f"SELECT Nombre, Email FROM \"{T_GSI}\" WHERE UsuarioID = 'U300'"
    )
    print(f"   - Select U300: {resp3.get('Items')}")
    
    # Update con PartiQL (Actualizamos el nombre de Ana)
    dynamodb_client.execute_statement(
        Statement=f"UPDATE \"{T_GSI}\" SET Nombre = 'Ana Sofia' WHERE UsuarioID = 'U100'"
    )
    print("   - Update U100 con PartiQL.")


def main():
    print("--- INICIANDO GESTIÓN DE DYNAMODB ---")
    crear_tablas()
    crear_registros()
    operaciones_crud_basicas()
    obtener_todos_scan()
    filtrado_scan_gsi()
    eliminacion_condicional()
    filtros_complejos_lsi()
    uso_partiql()
    print("\n--- FIN GESTIÓN DYNAMODB ---")

if __name__ == "__main__":
    main()