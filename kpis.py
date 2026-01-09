# Nota: En VS Code no necesitas la función main() obligatoriamente como en la web
# pero es buena práctica para organizar.
from snowflake.snowpark import Session
from snowflake.snowpark.functions import col, stddev, avg
import os
import certifi
os.environ['SSL_CERT_FILE'] = certifi.where()
env_variables = os.getenv()

def get_session():
    # Esta función busca la conexión activa en tu extensión de VS Code
    # Si no funciona, te enseñaré a poner las credenciales fijas
    try:
        from snowflake.app.devpy.connection import get_active_session
        return get_active_session()
    except ImportError:
        # Si falla, usamos una configuración manual (esto es lo más seguro en local)
        connection_parameters = {
            "account": env_variables.get("SNOWFLAKE_ACCOUNT"),
            "user": env_variables.get("SNOWFLAKE_USER"),
            "password": env_variables.get("SNOWFLAKE_PASSWORD"), # O usa un gestor de secretos
            "role": env_variables.get("SNOWFLAKE_ROLE"),
            "warehouse": env_variables.get("SNOWFLAKE_WAREHOUSE"),
            "database": env_variables.get("SNOWFLAKE_DATABASE"),
            "schema": env_variables.get("SNOWFLAKE_SCHEMA"),
            "ocsp_fail_open": True
        }
        return Session.builder.configs(connection_parameters).create()

def create_anomalies_report(session):
    #session.use_role("ROLE_OT_ADMIN")
    session.use_warehouse("WH_GOBERNANCE_XS")
    session.use_database("OT_CURATED")
    session.use_schema("ANALYTICS")

    df = session.table("OT_CLEAN.TRANSFORMED.TORRE_PYTHON_CLEAN")

    # Calculamos media y desviación estándar de la presión
    stats = df.select(
        avg(col("PRESION_ENTRADA")).alias("media"),
        stddev(col("PRESION_ENTRADA")).alias("desviacion")
    ).collect()[0]

    media = stats['media']
    desv = stats['desviacion']

    # Marcamos como anomalía cualquier valor > 2 desviaciones estándar
    df_anomalies = df.filter(col("PRESION_ENTRADA") > (media + 2 * desv)) \
                     .select("FECHA_REGISTRO", "PRESION_ENTRADA", "FAMILIA_PRODUCTO")

    # Guardamos el reporte de alertas
    df_anomalies.write.mode("overwrite").save_as_table("ALERTAS_PRESION_TORRE")
    
    return df_anomalies

# Para ejecutarlo en VS Code, asegúrate de tener la sesión activa
if __name__ == "__main__":
    # La extensión de VS Code maneja la sesión por ti al ejecutar
    session = get_session()
    create_anomalies_report(session)
    session.close()
    print("Ejecutando detección de anomalías...")