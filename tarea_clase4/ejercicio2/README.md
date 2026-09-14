# Ejercicio: Celery Queues y Airflow Pools en Apache Airflow

## 1. Descripción del escenario

Una compañía de seguros cuenta con una plataforma de procesamiento operacional que ejecuta diferentes procesos sobre información de clientes, pólizas y operaciones.

Los procesos son independientes entre sí, pero algunos tienen mayor prioridad para el negocio. Además, todos necesitan acceder a una misma base de datos operacional, la cual tiene una capacidad limitada para atender escrituras concurrentes.

El objetivo de la solución es utilizar:

- **Celery Queues** para separar los procesos según su prioridad y asignar los procesos críticos a un worker dedicado.
- **Airflow Pools** para controlar la cantidad de procesos que pueden acceder simultáneamente a la base de datos operacional.

Se implementaron cinco procesos independientes:

| Proceso | Prioridad | Queue | Pool |
|---|---|---|---|
| Procesar siniestros | Alta | `procesos_prioritarios` | `bd_operacional` |
| Actualizar pólizas | Alta | `procesos_prioritarios` | `bd_operacional` |
| Procesar reembolsos | Alta | `procesos_prioritarios` | `bd_operacional` |
| Generar reportes | Normal | `default` | `bd_operacional` |
| Actualizar indicadores | Normal | `default` | `bd_operacional` |

---

# 2. Procesos independientes

Los cinco procesos pueden ejecutarse de manera independiente porque no existe una dependencia entre ellos.

### Procesar siniestros

Procesa los siniestros registrados por los clientes y actualiza la información correspondiente en la base de datos.

### Actualizar pólizas

Actualiza la información operacional de las pólizas.

### Procesar reembolsos

Procesa las solicitudes de reembolso y registra su información en la base de datos.

### Generar reportes

Genera información para reportes comerciales y operacionales.

### Actualizar indicadores

Actualiza indicadores utilizados para seguimiento de clientes y operaciones.

Ninguno de estos procesos necesita esperar el resultado de otro proceso para comenzar su ejecución.

---

# 3. Procesos de mayor prioridad

Los tres procesos considerados críticos son:

1. `procesar_siniestros`
2. `actualizar_polizas`
3. `procesar_reembolsos`

Estos procesos tienen prioridad porque están relacionados directamente con operaciones que afectan al cliente y a la continuidad operativa de la compañía.

Por ejemplo:

- Los siniestros representan solicitudes que requieren atención operacional.
- Las pólizas necesitan mantenerse actualizadas para reflejar correctamente la información del cliente.
- Los reembolsos representan operaciones financieras que requieren procesamiento oportuno.

Por este motivo, los tres procesos utilizan una cola específica:

```text
procesos_prioritarios
```

---

# 4. ¿Por qué utilizar una Celery Queue?

Una Celery Queue permite determinar **qué worker debe ejecutar una determinada tarea**.

En esta solución existen dos colas:

```text
procesos_prioritarios
default
```

Los procesos críticos utilizan:

```python
queue="procesos_prioritarios"
```

Los procesos normales utilizan:

```python
queue="default"
```

Además, se creó un worker dedicado exclusivamente a la cola prioritaria.

Su configuración principal es:

```yaml
command: celery worker --queues procesos_prioritarios
```

De esta forma, el worker prioritario procesa únicamente las tareas enviadas a:

```text
procesos_prioritarios
```

Mientras tanto, el worker general continúa procesando las tareas de la cola:

```text
default
```

### ¿Cuándo utilizar Queue?

Una Queue es adecuada cuando necesitamos:

- Separar diferentes tipos de trabajo.
- Asignar determinados procesos a workers específicos.
- Dar prioridad operativa a determinados procesos.
- Evitar que procesos críticos dependan exclusivamente de los workers generales.

En este caso, la Queue permite que los procesos críticos tengan un worker dedicado.

---

# 5. Recurso compartido

Los cinco procesos necesitan acceder a una misma:

```text
Base de datos operacional
```

La base de datos es un recurso compartido.

Si todos los procesos realizaran escrituras simultáneamente, podrían producirse problemas como:

- Saturación de conexiones.
- Mayor consumo de CPU y memoria.
- Incremento de la latencia.
- Contención de recursos.
- Timeouts.
- Degradación del rendimiento.
- Fallos en las operaciones de escritura.

Por esta razón, además de separar los procesos mediante Queues, es necesario controlar el acceso concurrente a la base de datos.

Para esto se utiliza un Airflow Pool.

---

# 6. Airflow Pool

Se creó un Pool llamado:

```text
bd_operacional
```

con:

```text
2 slots
```

El comando utilizado es:

```bash
docker compose exec airflow-scheduler airflow pools set bd_operacional 2 "Protege la base de datos operacional compartida"
```

Las cinco tareas utilizan este Pool mediante:

```python
pool="bd_operacional"
```

Por lo tanto, aunque existan cinco tareas disponibles para ejecutarse, solamente dos pueden ocupar simultáneamente un slot del Pool.

---

# 7. ¿Por qué se eligieron 2 slots?

El Pool se configuró con **2 slots** porque se asume que la base de datos operacional puede soportar como máximo dos operaciones concurrentes de estos procesos sin afectar significativamente su rendimiento.

La elección permite demostrar claramente el control de concurrencia.

Existen:

```text
3 procesos prioritarios
+
2 procesos normales
=
5 procesos
```

Pero el recurso compartido solamente permite:

```text
2 accesos concurrentes
```

Por lo tanto, si las cinco tareas están listas para ejecutarse, como máximo dos podrán estar utilizando el recurso protegido al mismo tiempo.

Por ejemplo:

```text
Pool: bd_operacional
Slots disponibles: 2

procesar_siniestros    → SLOT 1 → ejecutando
actualizar_polizas     → SLOT 2 → ejecutando

procesar_reembolsos    → esperando
generar_reportes       → esperando
actualizar_indicadores → esperando
```

Cuando una de las tareas termina y libera su slot:

```text
procesar_siniestros → finaliza
                       ↓
                    SLOT libre
                       ↓
procesar_reembolsos → puede ejecutarse
```

De esta forma, el Pool evita que las cinco tareas accedan simultáneamente a la base de datos.

---

# 8. Diferencia entre Celery Queue y Airflow Pool

La solución demuestra que Queue y Pool tienen objetivos diferentes.

| Mecanismo | Qué controla | Aplicación |
|---|---|---|
| Celery Queue | Qué worker ejecuta la tarea | Procesos prioritarios |
| Airflow Pool | Cuántas tareas pueden utilizar un recurso | Base de datos |
| Queue | Distribución del trabajo | `procesos_prioritarios` |
| Pool | Concurrencia sobre recurso compartido | `bd_operacional` |

### Queue responde:

> ¿Qué worker debe ejecutar esta tarea?

### Pool responde:

> ¿Cuántas tareas pueden utilizar este recurso simultáneamente?

Por ejemplo:

```python
@task(
    queue="procesos_prioritarios",
    pool="bd_operacional"
)
```

significa que:

1. La tarea debe ser atendida por un worker que escuche `procesos_prioritarios`.
2. Antes de utilizar el recurso protegido necesita obtener un slot del Pool `bd_operacional`.

---

# 9. DAG implementado

El DAG contiene cinco procesos independientes:

- Tres prioritarios.
- Dos normales.

```python
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
    # EJECUCIÓN INDEPENDIENTE
    # ==========================================

    procesar_siniestros()
    actualizar_polizas()
    procesar_reembolsos()
    generar_reportes()
    actualizar_indicadores()


procesamiento_operacional_seguros()
```

---

# 10. Configuración del worker dedicado

Se agregó un worker dedicado para los procesos prioritarios mediante `docker-compose.override.yml`.

La configuración principal es:

```yaml
services:

  airflow-worker-prioritarios:
    image: ${AIRFLOW_IMAGE_NAME:-apache/airflow:3.0.0}

    command: celery worker --queues procesos_prioritarios

    environment:
      AIRFLOW__CORE__EXECUTOR: CeleryExecutor
      AIRFLOW__DATABASE__SQL_ALCHEMY_CONN: postgresql+psycopg2://airflow:airflow@postgres/airflow
      AIRFLOW__CELERY__RESULT_BACKEND: db+postgresql://airflow:airflow@postgres/airflow
      AIRFLOW__CELERY__BROKER_URL: redis://:@redis:6379/0
      AIRFLOW__CELERY__WORKER_CONCURRENCY: 2

      AIRFLOW__CORE__EXECUTION_API_SERVER_URL: 'http://airflow-apiserver:8080/execution/'

    volumes:
      - ${AIRFLOW_PROJ_DIR:-.}/dags:/opt/airflow/dags
      - ${AIRFLOW_PROJ_DIR:-.}/logs:/opt/airflow/logs
      - ${AIRFLOW_PROJ_DIR:-.}/config:/opt/airflow/config
      - ${AIRFLOW_PROJ_DIR:-.}/plugins:/opt/airflow/plugins

    user: "${AIRFLOW_UID:-50000}:0"

    depends_on:
      redis:
        condition: service_healthy
      postgres:
        condition: service_healthy
      airflow-apiserver:
        condition: service_healthy
      airflow-init:
        condition: service_completed_successfully

    restart: always
```

La línea fundamental es:

```yaml
command: celery worker --queues procesos_prioritarios
```

Esto hace que el worker consuma específicamente la cola:

```text
procesos_prioritarios
```

El worker general continúa atendiendo la cola `default`.

---

# 11. Arquitectura final

La arquitectura implementada puede representarse de la siguiente manera:

```text
                         AIRFLOW
                            │
                            ▼
              procesamiento_operacional_seguros
                            │
            ┌───────────────┴────────────────┐
            │                                │
            ▼                                ▼
   procesos_prioritarios                  default
            │                                │
            ▼                                ▼
 Worker prioritario                  Worker general
      concurrency 2
            │                                │
            └───────────────┬────────────────┘
                            │
                            ▼
                  Pool: bd_operacional
                         2 slots
                            │
                  ┌─────────┴─────────┐
                  │                   │
                  ▼                   ▼
               SLOT 1              SLOT 2
                  │                   │
                  └─────────┬─────────┘
                            ▼
                    BD OPERACIONAL
```

Los procesos prioritarios son:

```text
procesar_siniestros
actualizar_polizas
procesar_reembolsos
```

Los procesos normales son:

```text
generar_reportes
actualizar_indicadores
```

Todos utilizan el mismo Pool.

---

# 12. Ejemplo de funcionamiento

Supongamos que las cinco tareas están listas al mismo tiempo.

El worker prioritario puede recibir:

```text
procesar_siniestros
actualizar_polizas
procesar_reembolsos
```

El worker general puede recibir:

```text
generar_reportes
actualizar_indicadores
```

Sin embargo, el Pool solamente tiene dos slots:

```text
bd_operacional = 2
```

Por ejemplo:

```text
                POOL BD OPERACIONAL
                    2 SLOTS

             ┌───────────────┐
             │               │
             ▼               ▼
       Siniestros         Pólizas
        SLOT 1            SLOT 2
             │               │
             └───────┬───────┘
                     │
                     ▼
              BD OPERACIONAL


       Reembolsos
       Reportes
       Indicadores

       deben esperar
       slot disponible
```

Cuando cualquiera de las dos primeras tareas termina, el siguiente proceso obtiene el slot disponible.

Esto permite mantener la concurrencia del procesamiento sin sobrecargar la base de datos.

---

# 13. ¿Por qué no utilizar solamente Queue?

Una Queue permite separar los procesos y asignarlos a workers diferentes, pero no limita por sí misma el número de tareas que pueden acceder simultáneamente a la base de datos.

Por ejemplo, el worker prioritario podría tener tres tareas disponibles:

```text
procesar_siniestros
actualizar_polizas
procesar_reembolsos
```

Sin un Pool, las tres podrían intentar utilizar simultáneamente la base de datos.

Por lo tanto, Queue por sí sola no protege el recurso compartido.

---

# 14. ¿Por qué no utilizar solamente Pool?

El Pool limita el acceso concurrente a la base de datos, pero no permite separar los procesos por prioridad.

Sin Queue, los procesos prioritarios y normales podrían ser atendidos por los mismos workers.

La Queue permite crear un worker especializado para los procesos críticos.

Por lo tanto:

```text
Queue → separación y asignación de workers
Pool  → protección del recurso compartido
```

---

# 15. Relación entre concurrencia del worker y Pool

Es importante distinguir entre ambos conceptos.

El worker prioritario tiene:

```text
WORKER_CONCURRENCY = 2
```

Esto indica que puede ejecutar hasta dos tareas concurrentemente.

Pero el Pool también tiene:

```text
bd_operacional = 2 slots
```

Por lo tanto, el número de accesos simultáneos a la base de datos queda controlado por el Pool.

El Pool actúa como una segunda capa de protección.

Por ejemplo:

```text
Worker prioritario
       │
       │ hasta 2 tareas
       ▼
Pool bd_operacional
       │
       │ máximo 2 accesos
       ▼
Base de datos
```

El valor de `worker_concurrency` está relacionado con la capacidad de procesamiento del worker, mientras que los slots del Pool están relacionados con la capacidad del recurso compartido.

---

# 16. Comandos utilizados

### Levantar los servicios

```bash
docker compose up -d
```

### Detener los servicios

```bash
docker compose down
```

### Verificar los containers

```bash
docker compose ps
```

### Crear/configurar el Pool

```bash
docker compose exec airflow-scheduler airflow pools set bd_operacional 2 "Protege la base de datos operacional compartida"
```

### Ver los Pools

```bash
docker compose exec airflow-scheduler airflow pools list
```

### Revisar el worker prioritario

```bash
docker compose logs --tail=20 airflow-worker-prioritarios
```

---

# 17. Cumplimiento de los requisitos del ejercicio

| Requisito | Implementación |
|---|---|
| Escenario diferente de TiendaNova | Plataforma operacional de una compañía de seguros |
| Al menos 4 procesos independientes | 5 procesos |
| Procesos de mayor prioridad | Siniestros, pólizas y reembolsos |
| Queue dedicada | `procesos_prioritarios` |
| Worker dedicado | `airflow-worker-prioritarios` |
| Recurso compartido | Base de datos operacional |
| Riesgo de saturación | Escrituras concurrentes sobre la BD |
| Airflow Pool | `bd_operacional` |
| Slots justificados | 2 slots |
| DAG completo | `procesamiento_operacional_seguros` |
| Docker Compose delta | `docker-compose.override.yml` |
| Comando de creación del Pool | Incluido |
| Justificación de Queue | Separación/priorización de workers |
| Justificación de Pool | Protección de recurso compartido |

---

# 18. Conclusión

La solución demuestra de forma práctica el uso combinado de **Celery Queues** y **Airflow Pools**, utilizando cada mecanismo para resolver un problema diferente.

La **Queue `procesos_prioritarios`** permite separar los procesos críticos de los procesos normales y asignarlos a un worker dedicado.

El **Pool `bd_operacional`**, configurado con dos slots, limita la cantidad de tareas que pueden utilizar simultáneamente la base de datos operacional.

La existencia de tres procesos prioritarios y dos procesos normales permite demostrar que puede existir un número mayor de tareas disponibles que la capacidad de acceso simultáneo del recurso compartido.

En resumen:

```text
Celery Queue
    ↓
¿En qué worker debe ejecutarse?

Airflow Pool
    ↓
¿Cuántas tareas pueden utilizar simultáneamente el recurso?
```

Por lo tanto, la implementación no utiliza Queue y Pool solamente como elementos de configuración, sino que cada uno responde a una necesidad concreta del escenario de negocio.