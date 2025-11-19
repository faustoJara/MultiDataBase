import mysql.connector
import os
import sys

# === CONFIGURACIÓN DE MYSQL ===
DB_HOST = os.getenv("MYSQL_HOST", "localhost")
DB_NAME = os.getenv("MYSQL_DB", "CENTROCUIDADOFAMILIAR")
DB_USER = os.getenv("MYSQL_USER", "root")
DB_PASS = os.getenv("MYSQL_PASSWORD")

def main():
    conn = None
    try:
        print(f"--- Conectando a MySQL ({DB_HOST}) Base de Datos: {DB_NAME} ---")
        conn = mysql.connector.connect(host=DB_HOST, user=DB_USER, password=DB_PASS, database=DB_NAME)
        cursor = conn.cursor()
        
        # 1. CREAR SOLO LA TABLA NUEVA PARA EL EJERCICIO (Si no existe)
        # Esta tabla recibirá los datos calculados desde Redis al final
        sql_registro_tiempo = """
        CREATE TABLE IF NOT EXISTS REGISTRO_TIEMPO (
            IDRegistro INT PRIMARY KEY AUTO_INCREMENT,
            IDServicioOriginal INT UNIQUE,
            NombreUsuario VARCHAR(100),
            NombreCuidador VARCHAR(100),
            NombreCentro VARCHAR(100),
            DuracionMinutos INT,
            FechaCalculo DATETIME DEFAULT CURRENT_TIMESTAMP
        );
        """
        cursor.execute(sql_registro_tiempo)
        conn.commit()
        print("✅ Tabla 'REGISTRO_TIEMPO' verificada/creada correctamente.")

        # 2. VERIFICAR DATOS EXISTENTES (Solo lectura)
        # Comprobamos cuántos datos tienes ya en tu MySQL real
        tablas_a_verificar = ["USUARIA", "CUIDADOR", "CENTRO", "SERVICIO"]
        
        print("\n--- Estado actual de tus datos en MySQL ---")
        hay_datos = True
        for tabla in tablas_a_verificar:
            try:
                cursor.execute(f"SELECT COUNT(*) FROM {tabla}")
                count = cursor.fetchone()[0]
                print(f" -> Tabla {tabla}: {count} registros encontrados.")
                if tabla == "SERVICIO" and count == 0:
                    hay_datos = False
                    print("    ⚠️ ADVERTENCIA: No hay SERVICIOS. El cálculo de tiempo no generará nada.")
            except mysql.connector.Error as err:
                print(f" -> Tabla {tabla}: Error al leer ({err})")

        if hay_datos:
            print("\n✅ Todo listo. Redis podrá leer estos datos para calcular los tiempos.")
        else:
            print("\n⚠️ Faltan datos en la tabla SERVICIO. Asegúrate de que tu otro contenedor haya rellenado la BD.")

    except mysql.connector.Error as err:
        print(f"❌ Error Fatal de conexión MySQL: {err}")
        sys.exit(1)
    finally:
        if conn and conn.is_connected():
            conn.close()
            print("\nConexión cerrada.")

if __name__ == "__main__":
    main()