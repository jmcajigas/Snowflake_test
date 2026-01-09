# kpis.py (limpio para Snowflake)
from snowflake.snowpark.functions import col, stddev, avg

def run_anomalies(session):
    # Ya no necesitamos configurar la sesión, Snowflake nos la entrega
    df = session.table("OT_CLEAN.TRANSFORMED.TORRE_PYTHON_CLEAN")

    # Calculamos media y desviación estándar
    stats = df.select(
        avg(col("PRESION_ENTRADA")).alias("media"),
        stddev(col("PRESION_ENTRADA")).alias("desviacion")
    ).collect()[0]

    media = stats['media']
    desv = stats['desviacion']

    # Filtramos anomalías
    df_anomalies = df.filter(col("PRESION_ENTRADA") > (media + 2 * desv)) \
                     .select("FECHA_REGISTRO", "PRESION_ENTRADA", "FAMILIA_PRODUCTO")

    # Guardamos el resultado en CURATED
    df_anomalies.write.mode("overwrite").save_as_table("OT_CURATED.ANALYTICS.ALERTAS_PRESION_TORRE")
    
    return f"Proceso terminado. Se detectaron {df_anomalies.count()} anomalías."