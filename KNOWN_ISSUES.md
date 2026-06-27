# Known Issues

A running list of environment-specific quirks (mostly Windows + Docker Desktop)
and how to work around them.

---

## 1. Airflow DAG disappears from the UI (Airflow 3.0 standalone)

**Symptom**
- The DAG (`ecommerce_medallion_pipeline`) is missing from the Airflow UI.
- It briefly reappears after running `airflow dags reserialize`, then vanishes
  again on the next processing cycle.

**Cause** — *not your DAG or pipeline code.*
This is an Airflow 3.0 **standalone dag-processor** issue triggered by the host
machine **sleeping/hibernating** while the stack is left running. Docker Desktop
on Windows runs inside a WSL2 VM; when the laptop sleeps, that VM is paused.

After waking, the dag-processor mis-tracks how long each parse subprocess has
been alive. In `airflow/dag_processing/manager.py`:

```python
duration = now - processor.start_time
if duration > self.processor_timeout:   # default 50s
    # "Processor for <file> ... started <N> ago killing it."
```

`duration` comes out as a large constant (e.g. ~92,304s ≈ 25.6h — roughly the
time the machine was asleep), so **every** background parse is killed before the
DAG can be registered. Manual `reserialize` parses synchronously (bypasses the
killer) → DAG shows → background processor keeps failing → DAG goes stale →
disappears.

Confirmed not a code problem:
- Direct parse of the DAG file takes ~3s with no import errors.
- Kernel clock is consistent (`btime + uptime == now`, skew = 0).
- `dag` table shows `is_paused=f`, `is_stale=f` once a clean cycle runs.

**Fix (in order)**

1. **Restart Airflow and wait one refresh cycle (~5 min):**
   ```bash
   docker compose restart airflow
   ```
   The "killing it" errors stop and the DAG stays visible.

2. **If a restart isn't enough, reset the WSL2 VM clock** (PowerShell on the
   host), then bring the stack back up:
   ```powershell
   wsl --shutdown
   ```
   ```bash
   docker compose up -d
   ```

**Prevention**
- Stop the stack before long sleeps: `docker compose stop` (or `down`).
- Don't leave Airflow running overnight on a laptop that hibernates.

**Verify it's healthy**
```bash
docker exec airflow airflow dags list                 # DAG listed, is_paused False
docker logs --since 120s airflow | grep -c "killing it"   # should be 0
```

---

## 2. DAGs created in a *paused* state (Airflow 3.0)

**Symptom** — DAG exists but is greyed out / not running.

**Cause** — Airflow 3.0 pauses new DAGs at creation by default.

**Fix** — already configured in `docker-compose.yml`:
```yaml
AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION: "False"
```
This only affects *future* registrations. To unpause an already-paused DAG once:
```bash
docker exec airflow airflow dags unpause ecommerce_medallion_pipeline
```

---

## 3. Parallel bronze tasks exhaust memory

**Symptom** — Running many `landing_to_bronze` tasks in parallel OOM-kills the
jupyter container.

**Cause** — Each task starts its own `local` Spark JVM inside the single jupyter
container. Running all of them at once oversubscribes CPU/RAM.

**Fix** — already configured:
- `airflow/dags/ecommerce_medallion_dag.py`: `max_active_tasks=2` (cap concurrent
  bronze tasks; raise gradually while watching `docker stats`).
- `spark/jobs/landing_to_bronze.py`: `master("local[2]")`,
  `spark.driver.memory=1g`, `spark.sql.shuffle.partitions=8` (bound each job).

---

## Handy commands

```bash
docker compose ps                 # service health
docker stats                      # live CPU/memory per container
docker logs airflow               # standalone (api-server + scheduler + processor)
docker exec airflow airflow dags list
docker exec airflow airflow dags trigger ecommerce_medallion_pipeline
```

Airflow standalone admin password (regenerated on each airflow restart):
```bash
docker exec airflow cat $AIRFLOW_HOME/simple_auth_manager_passwords.json.generated
```
