from __future__ import annotations

import csv
import json
import logging
from datetime import datetime, timezone
from pathlib import Path

from airflow.sdk import dag, task, Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2.extras import execute_values


CSV_PATH = Path("/opt/airflow/data/massive/nvda/NVDA_aggs.csv")
SYMBOL = "NVDA"

POSTGRES_CONN_ID = "market_postgres"
TABLE_NAME = "nvda_aggs_minute"

VAR_DB_LAST_TS_MS = "massive_nvda_db_last_ts_ms"


def _to_float(x: str | None):
    if x is None or x == "":
        return None
    return float(x)


def _to_int(x: str | None):
    if x is None or x == "":
        return None
    return int(float(x))


def _to_bool(x: str | None):
    if x is None or x == "":
        return None
    s = str(x).strip().lower()
    if s in ("true", "t", "1", "yes", "y"):
        return True
    if s in ("false", "f", "0", "no", "n"):
        return False
    return None


@dag(
    dag_id="load_nvda_csv_to_market_postgres",
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["massive", "stocks", "postgres"],
)
def load_nvda_csv_to_market_postgres():

    @task
    def upsert_csv(batch_size: int = 5000) -> dict:
        # ========== CONFIGURAR LOGGER ==========
        log = logging.getLogger(__name__)
        log.info("="*80)
        log.info("INICIANDO CARGA DE CSV A POSTGRES")
        log.info("="*80)
        
        # ========== VALIDAR CSV ==========
        if not CSV_PATH.exists():
            raise FileNotFoundError(f"CSV no existe: {CSV_PATH}")
        
        if CSV_PATH.stat().st_size == 0:
            raise FileNotFoundError(f"CSV está vacío: {CSV_PATH}")
        
        csv_size_mb = CSV_PATH.stat().st_size / (1024 * 1024)
        log.info(f"📄 CSV encontrado: {CSV_PATH}")
        log.info(f"📊 Tamaño del archivo: {csv_size_mb:.2f} MB")
        
        # ========== OBTENER ÚLTIMO TIMESTAMP PROCESADO ==========
        last_ts_ms = Variable.get(VAR_DB_LAST_TS_MS, default=None)
        last_ts_ms = int(last_ts_ms) if last_ts_ms not in (None, "") else None
        
        if last_ts_ms is None:
            log.info("🆕 Primera carga - procesando todos los registros")
        else:
            last_dt = datetime.fromtimestamp(last_ts_ms / 1000.0, tz=timezone.utc)
            log.info(f"🔄 Carga incremental desde timestamp: {last_ts_ms}")
            log.info(f"   Última fecha procesada: {last_dt}")

        # ========== CONECTAR A POSTGRES ==========
        log.info(f"🔌 Conectando a PostgreSQL: {POSTGRES_CONN_ID}")
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = hook.get_conn()
        conn.autocommit = False

        # ========== CREAR TABLA SI NO EXISTE ==========
        log.info(f"🏗️  Verificando/creando tabla: {TABLE_NAME}")
        with conn.cursor() as cur:
            cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
              symbol TEXT NOT NULL,
              ts_ms BIGINT NOT NULL,
              ts TIMESTAMPTZ NOT NULL,
              open DOUBLE PRECISION,
              high DOUBLE PRECISION,
              low DOUBLE PRECISION,
              close DOUBLE PRECISION,
              volume DOUBLE PRECISION,
              vwap DOUBLE PRECISION,
              transactions BIGINT,
              otc BOOLEAN,
              raw JSONB,
              PRIMARY KEY (symbol, ts_ms)
            );
            """)
        conn.commit()
        log.info("✅ Tabla verificada/creada correctamente")

        # ========== PREPARAR QUERY DE UPSERT ==========
        sql = f"""
        INSERT INTO {TABLE_NAME}
          (symbol, ts_ms, ts, open, high, low, close, volume, vwap, transactions, otc, raw)
        VALUES %s
        ON CONFLICT (symbol, ts_ms) DO UPDATE SET
          ts = EXCLUDED.ts,
          open = EXCLUDED.open,
          high = EXCLUDED.high,
          low = EXCLUDED.low,
          close = EXCLUDED.close,
          volume = EXCLUDED.volume,
          vwap = EXCLUDED.vwap,
          transactions = EXCLUDED.transactions,
          otc = EXCLUDED.otc,
          raw = EXCLUDED.raw;
        """

        # ========== PROCESAR CSV ==========
        total = 0
        skipped = 0
        max_ts_ms_seen = last_ts_ms
        batch = []
        batch_count = 0
        
        log.info("📖 Comenzando lectura del CSV...")

        with CSV_PATH.open("r", newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)

            # Validar header
            expected = ["open","high","low","close","volume","vwap","timestamp","transactions","otc"]
            if reader.fieldnames != expected:
                raise ValueError(f"Header inesperado. Recibido: {reader.fieldnames}, esperado: {expected}")
            
            log.info(f"✅ Header del CSV validado correctamente")

            for row_num, row in enumerate(reader, start=1):
                ts_ms = _to_int(row.get("timestamp"))
                if ts_ms is None:
                    continue

                # Skip registros ya procesados
                if last_ts_ms is not None and ts_ms <= last_ts_ms:
                    skipped += 1
                    continue

                ts = datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc)

                # Logear primera fila de cada batch
                if len(batch) == 0:
                    batch_count += 1
                    log.info(f"📦 Batch #{batch_count} - Primera fila:")
                    log.info(f"   Timestamp: {ts} ({ts_ms})")
                    log.info(f"   Open: {row.get('open')}, Close: {row.get('close')}")
                    log.info(f"   High: {row.get('high')}, Low: {row.get('low')}")
                    log.info(f"   Volume: {row.get('volume')}")

                batch.append((
                    SYMBOL,
                    ts_ms,
                    ts,
                    _to_float(row.get("open")),
                    _to_float(row.get("high")),
                    _to_float(row.get("low")),
                    _to_float(row.get("close")),
                    _to_float(row.get("volume")),
                    _to_float(row.get("vwap")),
                    _to_int(row.get("transactions")),
                    _to_bool(row.get("otc")),
                    json.dumps(row),
                ))

                if max_ts_ms_seen is None or ts_ms > max_ts_ms_seen:
                    max_ts_ms_seen = ts_ms

                # Insertar batch cuando alcanza el tamaño
                if len(batch) >= batch_size:
                    with conn.cursor() as cur:
                        execute_values(cur, sql, batch, page_size=batch_size)
                    conn.commit()
                    total += len(batch)
                    log.info(f"✅ Batch #{batch_count} insertado: {len(batch)} filas | Total acumulado: {total}")
                    batch.clear()

        # ========== INSERTAR ÚLTIMO BATCH ==========
        if batch:
            batch_count += 1
            with conn.cursor() as cur:
                execute_values(cur, sql, batch, page_size=len(batch))
            conn.commit()
            total += len(batch)
            log.info(f"✅ Último batch #{batch_count} insertado: {len(batch)} filas | Total acumulado: {total}")

        # ========== OBTENER ESTADÍSTICAS DE LA DB ==========
        log.info("📊 Obteniendo estadísticas de la base de datos...")
        with conn.cursor() as cur:
            cur.execute(f"""
                SELECT 
                    COUNT(*) as total_rows,
                    MIN(ts) as first_timestamp,
                    MAX(ts) as last_timestamp,
                    MIN(close) as min_price,
                    MAX(close) as max_price,
                    AVG(volume) as avg_volume
                FROM {TABLE_NAME}
                WHERE symbol = %s
            """, (SYMBOL,))
            stats = cur.fetchone()
            
            log.info("📈 ESTADÍSTICAS EN BASE DE DATOS:")
            log.info(f"   Total registros en tabla: {stats[0]:,}")
            log.info(f"   Primer timestamp: {stats[1]}")
            log.info(f"   Último timestamp: {stats[2]}")
            log.info(f"   Precio mínimo: ${stats[3]:.2f}" if stats[3] else "   Precio mínimo: N/A")
            log.info(f"   Precio máximo: ${stats[4]:.2f}" if stats[4] else "   Precio máximo: N/A")
            log.info(f"   Volumen promedio: {stats[5]:,.0f}" if stats[5] else "   Volumen promedio: N/A")

        conn.close()

        # ========== ACTUALIZAR VARIABLE CON ÚLTIMO TIMESTAMP ==========
        if max_ts_ms_seen is not None:
            Variable.set(VAR_DB_LAST_TS_MS, str(max_ts_ms_seen))
            last_datetime = datetime.fromtimestamp(max_ts_ms_seen / 1000.0, tz=timezone.utc)
            log.info(f"💾 Variable actualizada: {VAR_DB_LAST_TS_MS} = {max_ts_ms_seen}")
            log.info(f"   Corresponde a: {last_datetime}")

        # ========== RESUMEN FINAL ==========
        log.info("="*80)
        log.info("📋 RESUMEN DE LA CARGA")
        log.info("="*80)
        log.info(f"✅ Registros nuevos insertados: {total:,}")
        log.info(f"⏭️  Registros omitidos (ya existían): {skipped:,}")
        log.info(f"📦 Total de batches procesados: {batch_count}")
        log.info(f"📁 Archivo CSV: {CSV_PATH}")
        log.info(f"🗄️  Tabla destino: {TABLE_NAME}")
        log.info(f"🔢 Tamaño del batch: {batch_size:,}")
        log.info("="*80)

        # ========== RETORNAR RESULTADO ==========
        result = {
            "rows_inserted": total,
            "rows_skipped": skipped,
            "batches_processed": batch_count,
            "last_timestamp_ms": max_ts_ms_seen,
            "last_timestamp_dt": datetime.fromtimestamp(max_ts_ms_seen / 1000.0, tz=timezone.utc).isoformat() if max_ts_ms_seen else None,
            "csv_path": str(CSV_PATH),
            "csv_size_mb": round(csv_size_mb, 2),
            "table": TABLE_NAME,
            "db_total_rows": stats[0] if stats else None
        }
        
        log.info(f"📤 Resultado final: {json.dumps(result, indent=2)}")
        return result

    upsert_csv()


load_nvda_csv_to_market_postgres()
