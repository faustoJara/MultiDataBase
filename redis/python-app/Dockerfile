# Usamos una imagen base de Python
FROM python:3.10-slim

# Establecemos el directorio de trabajo dentro del contenedor
WORKDIR /app

# Actualizamos pip
RUN pip install --upgrade pip

# Copiamos el archivo de requisitos e instalamos las dependencias
COPY requirements.txt .
RUN pip install -r requirements.txt

# Copiamos el resto del código de la aplicación
COPY . .

# El 'command' se define en docker-compose.yml