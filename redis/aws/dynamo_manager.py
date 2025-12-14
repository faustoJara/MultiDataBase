import boto3
import time
import json
from boto3.dynamodb.conditions import Key, Attr
from botocore.exceptions import ClientError

# === CONFIGURACIÓN ===
# Asegúrate de tener tus credenciales configuradas en AWS CLI o variables de entorno
REGION = 'us-east-1' # Cambia a tu región de AWS (ej: us-east-1)
dynamodb = boto3.resource('dynamodb', region_name=REGION)
dynamodb_client = boto3.client('dynamodb', region_name=REGION)

# Nombres de Tablas
T_SIMPLE = 'CentroCuidado_Log_Simple'      # Tabla 1: Sin índices secundarios
T_LSI = 'CentroCuidado_Pagos_LSI'          # Tabla 2: Con Índice Local (LSI)
T_GSI = 'CentroCuidado_Usuarios_GSI'       # Tabla 3: Con Índice Global (GSI)

def log(titulo, mensaje):
    print(f"\n{'='*50}\n  {titulo}\n{'='*50}")
    print(f" -> {mensaje}")
    time.sleep(1)

# --- 1. CREACIÓN DE TABLAS ---

def crear_tablas():
    log("CREANDO TABLAS", "Verificando y creando tablas en DynamoDB...")
    existing_tables = dynamodb_client.list_tables()['TableNames']

    # 1. Tabla Simple (Log de accesos) - PK: LogID
    if T_SIMPLE not in existing_tables:
        print(f"Creando {T_SIMPLE}...")
        dynamodb.create_table(
            TableName=T_SIMPLE,
            KeySchema=[{'AttributeName': 'LogID', 'KeyType': 'HASH'}],
            AttributeDefinitions=[{'AttributeName': 'LogID', 'AttributeType': 'S'}],
            ProvisionedThroughput={'ReadCapacityUnits': 5, 'WriteCapacityUnits': 5}
        )
    
    # 2. Tabla con LSI (Pagos) - PK: UsuarioID, SK: FechaPago. LSI: Monto
    if T_LSI not in existing_tables:
        print(f"Creando {T_LSI}...")
        dynamodb.create_table(
            TableName=T_LSI,
            KeySchema=[
                {'AttributeName': 'UsuarioID', 'KeyType': 'HASH'},
                {'AttributeName': 'FechaPago', 'KeyType': 'RANGE'}
            ],
            AttributeDefinitions=[
                {'AttributeName': 'UsuarioID', 'AttributeType': 'S'},
                {'AttributeName': 'FechaPago', 'AttributeType': 'S'},
                {'AttributeName': 'Monto', 'AttributeType': 'N'}
            ],
            LocalSecondaryIndexes=[{
                'IndexName': 'IndiceMonto',
                'KeySchema': [
                    {'AttributeName': 'UsuarioID', 'KeyType': 'HASH'},
                    {'AttributeName': 'Monto', 'KeyType': 'RANGE'}
                ],
                'Projection': {'ProjectionType': 'ALL'}
            }],
            ProvisionedThroughput={'ReadCapacityUnits': 5, 'WriteCapacityUnits': 5}
        )

    # 3. Tabla con GSI (Usuarios) - PK: UsuarioID. GSI: Email
    if T_GSI not in existing_tables:
        print(f"Creando {T_GSI}...")
        dynamodb.create_table(
            TableName=T_GSI,
            KeySchema=[{'AttributeName': 'UsuarioID', 'KeyType': 'HASH'}],
            AttributeDefinitions=[
                {'AttributeName': 'UsuarioID', 'AttributeType': 'S'},
                {'AttributeName': 'Email', 'AttributeType': 'S'}
            ],
            GlobalSecondaryIndexes=[{
                'IndexName': 'IndiceEmail',
                'KeySchema': [{'AttributeName': 'Email', 'KeyType': 'HASH'}],
                'Projection': {'ProjectionType': 'ALL'},
                'ProvisionedThroughput': {'ReadCapacityUnits': 5, 'WriteCapacityUnits': 5}
            }],
            ProvisionedThroughput={'ReadCapacityUnits': 5, 'WriteCapacityUnits': 5}
        )
    
    print("Esperando a que las tablas estén ACTIVAS (puede tardar 10-20 seg)...")
    time.sleep(10) # Espera prudencial

# --- 2. GESTIÓN DE DATOS ---

def crear_registros():
    log("INSERTAR DATOS", "Creando 3 registros en cada tabla...")
    
    # Tabla Simple
    t1 = dynamodb.Table(T_SIMPLE)
    t1.put_item(Item={'LogID': 'L1', 'Mensaje': 'Inicio sesión', 'Nivel': 'INFO'})
    t1.put_item(Item={'LogID': 'L2', 'Mensaje': 'Error conexión', 'Nivel': 'ERROR'})
    t1.put_item(Item={'LogID': 'L3', 'Mensaje': 'Logout', 'Nivel': 'INFO'})

    # Tabla LSI
    t2 = dynamodb.Table(T_LSI)
    t2.put_item(Item={'UsuarioID': 'U1', 'FechaPago': '2023-01-01', 'Monto': 100, 'Concepto': 'Mensualidad'})
    t2.put_item(Item={'UsuarioID': 'U1', 'FechaPago': '2023-02-01', 'Monto': 150, 'Concepto': 'Extra'})
    t2.put_item(Item={'UsuarioID': 'U2', 'FechaPago': '2023-01-05', 'Monto': 500, 'Concepto': 'Anual'})

    # Tabla GSI
    t3 = dynamodb.Table(T_GSI)
    t3.put_item(Item={'UsuarioID': 'U100', 'Email': 'ana@test.com', 'Nombre': 'Ana', 'Estado': 'Activo'})
    t3.put_item(Item={'UsuarioID': 'U200', 'Email': 'luis@test.com', 'Nombre': 'Luis', 'Estado': 'Inactivo'})
    t3.put_item(Item={'UsuarioID': 'U300', 'Email': 'maria@test.com', 'Nombre': 'Maria', 'Estado': 'Activo'})
    
    print("Registros insertados.")

def operaciones_crud_basicas():
    log("CRUD BÁSICO", "Realizando Get, Update y Delete...")
    t1 = dynamodb.Table(T_SIMPLE)

    # GET
    resp = t1.get_item(Key={'LogID': 'L1'})
    print(f"1. Obtener registro (L1): {resp.get('Item')}")

    # UPDATE
    t1.update_item(
        Key={'LogID': 'L1'},
        UpdateExpression="set Mensaje=:m",
        ExpressionAttributeValues={':m': 'Inicio sesión (Actualizado)'}
    )
    print("2. Registro L1 actualizado.")

    # DELETE
    t1.delete_item(Key={'LogID': 'L3'})
    print("3. Registro L3 eliminado.")

def obtener_todos_scan():
    log("SCAN (Obtener Todos)", "Obteniendo todos los registros de la tabla Simple.")
    t1 = dynamodb.Table(T_SIMPLE)
    resp = t1.scan()
    print(f"Registros encontrados: {resp['Items']}")

def filtrado_scan_gsi():
    log("SCAN CON FILTRO & GSI", "Filtrando datos en las tablas.")
    
    # Scan filtrado en tabla simple
    t1 = dynamodb.Table(T_SIMPLE)
    resp = t1.scan(FilterExpression=Attr('Nivel').eq('ERROR'))
    print(f"1. Scan Filtrado (Nivel=ERROR): {resp['Items']}")

    # Query usando GSI (Índice Global)
    t3 = dynamodb.Table(T_GSI)
    # Buscamos por Email (que no es la clave primaria, por eso usamos el índice)
    resp_gsi = t3.query(
        IndexName='IndiceEmail',
        KeyConditionExpression=Key('Email').eq('luis@test.com')
    )
    print(f"2. Búsqueda por Índice Global (Email=luis@test.com): {resp_gsi['Items']}")

def eliminacion_condicional():
    log("ELIMINACIÓN CONDICIONAL", "Intentando borrar datos con condiciones.")
    t3 = dynamodb.Table(T_GSI)
    
    try:
        # Intentamos borrar U100 SOLO SI su Estado es 'Inactivo' (Follará porque es Activo)
        print("Intentando borrar Ana (Activo) con condición Estado=Inactivo...")
        t3.delete_item(
            Key={'UsuarioID': 'U100'},
            ConditionExpression=Attr('Estado').eq('Inactivo')
        )
    except ClientError as e:
        print(f" -> Bloqueado por condición (Correcto): {e.response['Error']['Code']}")

    # Borramos Luis porque es Inactivo (Debe funcionar)
    t3.delete_item(
        Key={'UsuarioID': 'U200'},
        ConditionExpression=Attr('Estado').eq('Inactivo')
    )
    print(" -> Luis eliminado correctamente (cumplía condición).")

def filtros_complejos():
    log("FILTROS COMPLEJOS", "Filtrando Pagos (LSI) por usuario y monto > 120.")
    t2 = dynamodb.Table(T_LSI)
    
    # Usamos el índice local para buscar pagos de U1 mayores a 120
    resp = t2.query(
        IndexName='IndiceMonto',
        KeyConditionExpression=Key('UsuarioID').eq('U1') & Key('Monto').gt(120)
    )
    print(f"Pagos de U1 mayores a 120: {resp['Items']}")

def uso_partiql():
    log("PARTIQL STATEMENT", "Usando sintaxis SQL para consultar DynamoDB.")
    
    # Seleccionar de la tabla simple donde Nivel es ERROR
    # Nota: PartiQL requiere comillas dobles para nombres de tabla si tienen caracteres especiales
    try:
        resp = dynamodb_client.execute_statement(
            Statement=f'SELECT * FROM "{T_SIMPLE}" WHERE Nivel = \'ERROR\''
        )
        print(f"Resultado PartiQL (Select ERROR): {resp['Items']}")
        
        # Insertar con PartiQL
        dynamodb_client.execute_statement(
            Statement=f"INSERT INTO \"{T_SIMPLE}\" VALUE {{'LogID': 'L99', 'Mensaje': 'Creado con PartiQL', 'Nivel': 'WARN'}}"
        )
        print("Insertado registro L99 con PartiQL.")
        
    except Exception as e:
        print(f"Error PartiQL: {e}")

def main():
    print("--- INICIANDO GESTIÓN DE DYNAMODB ---")
    crear_tablas()
    crear_registros()
    operaciones_crud_basicas()
    obtener_todos_scan()
    filtrado_scan_gsi()
    eliminacion_condicional()
    filtros_complejos()
    uso_partiql()
    print("\n--- FIN GESTIÓN DYNAMODB ---")

if __name__ == "__main__":
    main()