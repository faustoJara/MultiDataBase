import mysql.connector
import os
import sys

# === CONFIGURACIÓN DE MYSQL ===
# Leemos las variables de entorno (pasadas por docker-compose)
DB_HOST = os.getenv("MYSQL_HOST", "localhost")
DB_NAME = os.getenv("MYSQL_DB", "servicios_db")
DB_USER = os.getenv("MYSQL_USER", "root")
DB_PASS = os.getenv("MYSQL_PASSWORD")

if not DB_PASS:
    print("Error: La variable de entorno MYSQL_PASSWORD no está definida.")
    sys.exit(1)

# --- TU ESQUEMA SQL (Ligeramente adaptado para Python) ---
MYSQL_TABLES_SQL = [
f"""
CREATE DATABASE IF NOT EXISTS {DB_NAME};
""",
f"""
USE {DB_NAME};
""",
"""
CREATE TABLE IF NOT EXISTS USUARIA (
    IDUsuario INT PRIMARY KEY AUTO_INCREMENT,
    Nombre VARCHAR(100) NOT NULL,
    Apellido VARCHAR(100) NOT NULL,
    DNI VARCHAR(20) UNIQUE NOT NULL,
    FechaNacimiento DATE,
    Barrio VARCHAR(100),
    RentaPercapita DECIMAL(10, 2), 
    Telefono VARCHAR(20),
    Email VARCHAR(100) UNIQUE,
    Genero VARCHAR(50) 
);
""",
"""
CREATE TABLE IF NOT EXISTS DEPENDIENTE (
    IDDependiente INT PRIMARY KEY AUTO_INCREMENT,
    IDUsuario INT NOT NULL,
    Nombre VARCHAR(100) NOT NULL,
    Apellido VARCHAR(100) NOT NULL,
    TipoDependencia VARCHAR(50),
    FechaNacimiento DATE,
    PerfilMedico TEXT, 
    FOREIGN KEY (IDUsuario) REFERENCES USUARIA(IDUsuario) ON DELETE CASCADE
);
""",
"""
CREATE TABLE IF NOT EXISTS CUIDADOR (
    IDCuidador INT PRIMARY KEY AUTO_INCREMENT,
    Nombre VARCHAR(100) NOT NULL,
    Apellido VARCHAR(100) NOT NULL,
    DNI VARCHAR(20) UNIQUE NOT NULL,
    Telefono VARCHAR(20),
    Disponibilidad VARCHAR(255),
    Especialidad VARCHAR(100)
);
""",
"""
CREATE TABLE IF NOT EXISTS CENTRO (
    IDCentro INT PRIMARY KEY AUTO_INCREMENT,
    NombreCentro VARCHAR(100) NOT NULL,
    Direccion VARCHAR(255),
    DescripcionCentro TEXT,
    CapacidadMaxima INT
);
""",
"""
CREATE TABLE IF NOT EXISTS SERVICIO (
    IDServicio INT PRIMARY KEY AUTO_INCREMENT,
    IDUsuario INT NOT NULL,
    IDDependiente INT NOT NULL,
    IDCuidador INT,
    IDCentro INT,
    FechaHoraInicio DATETIME NOT NULL, 
    FechaHoraFin DATETIME NOT NULL,
    PrecioBase DECIMAL(10, 2) NOT NULL,
    SubvencionAplicada DECIMAL(10, 2) DEFAULT 0.00,
    PrecioFinal DECIMAL(10, 2) NOT NULL,
    Estado VARCHAR(50),
    
    FOREIGN KEY (IDUsuario) REFERENCES USUARIA(IDUsuario) ON DELETE CASCADE,
    FOREIGN KEY (IDDependiente) REFERENCES DEPENDIENTE(IDDependiente) ON DELETE CASCADE,
    FOREIGN KEY (IDCuidador) REFERENCES CUIDADOR(IDCuidador),
    FOREIGN KEY (IDCentro) REFERENCES CENTRO(IDCentro)
);
""",
"""
CREATE TABLE IF NOT EXISTS TRANSACCION (
    IDTransaccion INT PRIMARY KEY AUTO_INCREMENT,
    IDServicio INT UNIQUE NOT NULL,
    FechaTransaccion DATETIME DEFAULT CURRENT_TIMESTAMP, 
    Monto DECIMAL(10, 2) NOT NULL,
    EstadoPago VARCHAR(50),
    
    FOREIGN KEY (IDServicio) REFERENCES SERVICIO(IDServicio) ON DELETE CASCADE
);
""",
"""
CREATE TABLE IF NOT EXISTS RESENA (
    IDResena INT PRIMARY KEY AUTO_INCREMENT,
    IDServicio INT UNIQUE NOT NULL,
    IDUsuario INT NOT NULL,
    Puntuacion INT,
    Comentario TEXT,
    FechaResena DATETIME DEFAULT CURRENT_TIMESTAMP,
    
    FOREIGN KEY (IDServicio) REFERENCES SERVICIO(IDServicio) ON DELETE CASCADE,
    FOREIGN KEY (IDUsuario) REFERENCES USUARIA(IDUsuario) ON DELETE CASCADE
);
"""
]

# --- DATOS DE EJEMPLO PARA INSERTAR ---
DATA_USUARIA = [
    (1, 'Ana', 'García', '12345678A', '1985-05-15', 'Centro', 25000.00, '600111222', 'ana.garcia@email.com', 'Femenino'),
    (2, 'Luis', 'Martínez', '87654321B', '1990-11-20', 'Nervión', 32000.00, '600333444', 'luis.martinez@email.com', 'Masculino')
]
DATA_CUIDADOR = [
    (101, 'Carmen', 'Ruiz', '11112222C', '650101010', 'Lunes-Viernes Mañana', 'Geriatría'),
    (102, 'Javier', 'Sánchez', '33334444D', '650202020', 'Fines de Semana', 'Fisioterapia')
]
DATA_DEPENDIENTE = [
    (1, 1, 'Elena', 'García', 'Grado III', '1950-03-10', 'Hipertensión'),
    (2, 1, 'Marcos', 'García', 'Grado I', '2015-01-25', 'Asma')
]
DATA_CENTRO = [
    (1, 'Centro de Día Sol', 'Calle Sol 10', 'Centro de día para mayores', 50)
]

def main():
    try:
        # Conexión a MySQL (sin especificar la base de datos al inicio)
        conn = mysql.connector.connect(
            host=DB_HOST,
            user=DB_USER,
            password=DB_PASS
        )
        cursor = conn.cursor()
        print(f"Conexión a MySQL en {DB_HOST} exitosa.")
        
        # --- 1. CREAR ESTRUCTURA ---
        print(f"Creando base de datos {DB_NAME} y tablas...")
        for query in MYSQL_TABLES_SQL:
            try:
                # Usamos multi=True para manejar sentencias que podrían estar separadas
                for _ in cursor.execute(query, multi=True): pass
            except mysql.connector.Error as err:
                # Ignoramos errores si la tabla ya existe, etc.
                print(f"Advertencia al ejecutar: {err.msg}")
                
        conn.commit()
        # Ahora nos conectamos directamente a la BD
        conn.database = DB_NAME
        print(f"Esquema de tablas en '{DB_NAME}' verificado/creado.")

        # --- 2. INSERTAR DATOS DE EJEMPLO (LIMPIANDO PRIMERO) ---
        print("Limpiando datos antiguos e insertando nuevos...")
        
        # Desactivar foreign key checks para limpiar
        cursor.execute("SET FOREIGN_KEY_CHECKS = 0;")
        cursor.execute("TRUNCATE TABLE USUARIA;")
        cursor.execute("TRUNCATE TABLE CUIDADOR;")
        cursor.execute("TRUNCATE TABLE DEPENDIENTE;")
        cursor.execute("TRUNCATE TABLE CENTRO;")
        cursor.execute("TRUNCATE TABLE SERVICIO;") # Limpiamos todas
        cursor.execute("TRUNCATE TABLE TRANSACCION;")
        cursor.execute("TRUNCATE TABLE RESENA;")
        cursor.execute("SET FOREIGN_KEY_CHECKS = 1;")

        # Insertar
        cursor.executemany("INSERT INTO USUARIA VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)", DATA_USUARIA)
        cursor.executemany("INSERT INTO CUIDADOR VALUES (%s, %s, %s, %s, %s, %s, %s)", DATA_CUIDADOR)
        cursor.executemany("INSERT INTO DEPENDIENTE VALUES (%s, %s, %s, %s, %s, %s, %s)", DATA_DEPENDIENTE)
        cursor.executemany("INSERT INTO CENTRO VALUES (%s, %s, %s, %s, %s)", DATA_CENTRO)
        
        conn.commit()
        print(f"Insertados {len(DATA_USUARIA)} usuarias, {len(DATA_CUIDADOR)} cuidadores, {len(DATA_DEPENDIENTE)} dependientes, {len(DATA_CENTRO)} centros.")
        
        print("\nVerificando datos en MySQL (USUARIA):")
        cursor.execute("SELECT * FROM USUARIA")
        for row in cursor.fetchall():
            print(row)

    except mysql.connector.Error as err:
        print(f"Error al conectar o configurar MySQL: {err}")
        print(f"Asegúrate de que el contenedor MySQL esté corriendo en la red correcta y que las variables de entorno (HOST, USER, PASS, DB) sean correctas.")
        sys.exit(1)
    finally:
        if 'conn' in locals() and conn.is_connected():
            cursor.close()
            conn.close()
            print("\nConexión MySQL cerrada.")

if __name__ == "__main__":
    main()