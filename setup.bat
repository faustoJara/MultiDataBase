@echo off
ECHO --- 1. COMPROBANDO ENTORNO VIRTUAL 'venv' ---

IF NOT EXIST ".\venv\" (
    ECHO Creando entorno virtual en '.\venv\'...
    python -m venv venv
) ELSE (
    ECHO El entorno '.\venv\' ya existe. Omitiendo creacion.
)

ECHO.
ECHO --- 2. INSTALANDO DEPENDENCIAS ---
ECHO Usando pip desde el entorno virtual para instalar...

:: Llama al python/pip dentro del venv para instalar los paquetes
call ".\venv\Scripts\python.exe" -m pip install -r requirements.txt

ECHO.
ECHO --- ¡INSTALACION COMPLETADA! ---
ECHO.
ECHO ========================================================
ECHO  PASO SIGUIENTE: ¡Activa el entorno!
ECHO  Ejecuta este comando en tu terminal de PowerShell:
ECHO.
ECHO    .\venv\Scripts\Activate
ECHO ========================================================
ECHO.
PAUSE