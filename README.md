# MultiDataBase
Conexión con varias bases de datos, mySQL, postgreSQL, y MongoDB
En este proyecto:
Un documento explicando una idea de un negocio dónde se usa el big data y  que sirva para ayudar a la comunidad o a este instituto. El documento debe tener los siguientes apartados:
Introducción 
Aplicación de las 7Vs del Big Data
Clasificación del Dato 
Analítica del Negocio, debe investigarse una herramienta por cada metodología
Conclusiones
 
La segunda parte del trabajo es práctica:
Crear Diagrama Entidad-Relación con la Base de datos Estructurado de la aplicación al menos 10 tablas con sus columnas 
Crear Base de datos con el diagrama Entidad-Relación anterior en un docker con Mysql y conectarlo con DBeaver
Crear Base de datos con el diagrama Entidad-Relación anterior en un docker con PostgreSQL y un docker con motor de base de datos que queraís y conectarlos con DBeaver
Realizar un script en python que rellene todas las bases de datos con Faker, pero debe usarse al menos 10 providers distintos de Faker para generar datos. Indicar al principio del scripts con comentarios los 10 comandos usados 
Realizar un script en python que cree las tres bases de datos entera en base al diagrama entidad-relación que habéis hecho.
Un script en python que obtenga los datos que considereis más importantes para analizar  y genere un archivo .json. NO QUIERO un json con todas las tablas y los registros de ella. QUIERO que hagáis consultas que unan varias tablas y obtengáis los datos que consideréis más importante. Explicar porque son importantes. Por cada base de datos quiero un .json distinto con datos distintos

Debéis entregarme lo siguiente para esta tarea:
Documento en PDF
Enlace a NotebookLM con el PDF que habéis creado con el proyecto creando todo lo que podáis sobre ese documento.
Documento con el Diagrama Entidad-Relación
Captura de pantalla de DBeaver con las tablas creadas en cada una de las bases de datos.
Enlace a github que contenga lo siguiente: Los tres dockerfile para crear las 3 bases de datos con sus volumenes correspondiente y los scripts en python
