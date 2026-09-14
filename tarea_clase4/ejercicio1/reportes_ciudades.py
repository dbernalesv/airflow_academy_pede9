# =============================================================================
# CLASE 4 - Ejercicio 1
# Enunciado: DAG con 6 tasks independientes (reportes de ciudades),
# cada una tarda ~10 segundos en correr (time.sleep(10)).
# Objetivo: medir tiempo total bajo distintas configuraciones de workers.
# =============================================================================

from airflow.decorators import dag, task
from datetime import datetime
import time

FECHA_INICIO = datetime(2026, 9, 1)

CIUDADES = ["lima", "arequipa", "trujillo", "chiclayo", "piura", "cusco"]

@dag(
    dag_id="reportes_ciudades",
    schedule=None,  # solo se dispara manualmente
    start_date=FECHA_INICIO,
    catchup=False,
    tags=["pede9", "clase4", "ejercicio1", "paralelismo"],
)
def reportes_ciudades():

    for ciudad in CIUDADES:
        @task(task_id=f"reporte_{ciudad}")
        def generar_reporte(ciudad=ciudad):
            print(f"[{ciudad}] generando reporte...")
            time.sleep(10)  # simula trabajo pesado
            print(f"[{ciudad}] reporte listo")
            return f"Reporte de {ciudad} completado"

        generar_reporte()

reportes_ciudades()