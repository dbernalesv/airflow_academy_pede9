from airflow.decorators import dag, task
from datetime import datetime

FECHA_INICIO = datetime(2026, 9, 1)

COLA_PRIORITARIA = "procesos_prioritarios"
COLA_DEFAULT = "default"
POOL_BD = "bd_operacional"


@dag(
    dag_id="procesamiento_operacional_seguros",
    schedule=None,
    start_date=FECHA_INICIO,
    catchup=False,
    tags=["pede9", "ejercicio2", "celery", "pool"],
)
def procesamiento_operacional_seguros():

    # ==========================================
    # PROCESOS PRIORITARIOS
    # ==========================================

    @task(
        queue=COLA_PRIORITARIA,
        pool=POOL_BD
    )
    def procesar_siniestros():
        import time

        print("=== PROCESANDO SINIESTROS ===")
        time.sleep(5)

        print("Escribiendo siniestros en BD...")
        time.sleep(5)

        print("Siniestros completados")


    @task(
        queue=COLA_PRIORITARIA,
        pool=POOL_BD
    )
    def actualizar_polizas():
        import time

        print("=== ACTUALIZANDO POLIZAS ===")
        time.sleep(5)

        print("Escribiendo pólizas en BD...")
        time.sleep(5)

        print("Pólizas completadas")


    @task(
        queue=COLA_PRIORITARIA,
        pool=POOL_BD
    )
    def procesar_reembolsos():
        import time

        print("=== PROCESANDO REEMBOLSOS ===")
        time.sleep(5)

        print("Escribiendo reembolsos en BD...")
        time.sleep(5)

        print("Reembolsos completados")


    # ==========================================
    # PROCESOS NORMALES
    # ==========================================

    @task(
        queue=COLA_DEFAULT,
        pool=POOL_BD
    )
    def generar_reportes():
        import time

        print("=== GENERANDO REPORTES ===")
        time.sleep(5)

        print("Escribiendo reportes en BD...")
        time.sleep(5)

        print("Reportes completados")


    @task(
        queue=COLA_DEFAULT,
        pool=POOL_BD
    )
    def actualizar_indicadores():
        import time

        print("=== ACTUALIZANDO INDICADORES ===")
        time.sleep(5)

        print("Escribiendo indicadores en BD...")
        time.sleep(5)

        print("Indicadores completados")


    # ==========================================
    # EJECUCION INDEPENDIENTE
    # ==========================================

    procesar_siniestros()
    actualizar_polizas()
    procesar_reembolsos()
    generar_reportes()
    actualizar_indicadores()


procesamiento_operacional_seguros()