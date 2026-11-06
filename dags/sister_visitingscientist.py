import json
import logging
from datetime import datetime, timedelta

from airflow.sdk import dag, task
from sister_common import get_db_connection, get_json

log = logging.getLogger(__name__)

# ID SDM dosen yang datanya diambil dari SISTER
ID_SDM = "516381bf-1fce-4874-ae5f-a6b8cc24c5b0"

# Endpoint SISTER: GET {{baseUrl}}/visiting_scientist?id_sdm=<uuid>
ENDPOINT = "visiting_scientist"

# Insert baru, atau update jika id sudah ada
QUERY = """
    INSERT INTO sister_visiting_scientist (
        id, id_sdm, kategori_kegiatan, perguruan_tinggi_pengundang,
        lama_kegiatan, tanggal_pelaksanaan, raw_response
    )
    VALUES (%s, %s, %s, %s, %s, %s, %s)
    ON DUPLICATE KEY UPDATE
        id_sdm = VALUES(id_sdm),
        kategori_kegiatan = VALUES(kategori_kegiatan),
        perguruan_tinggi_pengundang = VALUES(perguruan_tinggi_pengundang),
        lama_kegiatan = VALUES(lama_kegiatan),
        tanggal_pelaksanaan = VALUES(tanggal_pelaksanaan),
        raw_response = VALUES(raw_response)
"""

# DDL tabel tujuan (jalankan sekali di MariaDB):
#
# CREATE TABLE IF NOT EXISTS sister_visiting_scientist (
#     id                           VARCHAR(100) NOT NULL PRIMARY KEY,
#     id_sdm                       VARCHAR(100) NOT NULL,
#     kategori_kegiatan            VARCHAR(255) NULL,
#     perguruan_tinggi_pengundang  VARCHAR(255) NULL,
#     lama_kegiatan                INT          NULL,
#     tanggal_pelaksanaan          DATE         NULL,
#     raw_response                 JSON         NOT NULL,
#     updated_at                   TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP
#                                               ON UPDATE CURRENT_TIMESTAMP,
#     INDEX idx_id_sdm (id_sdm)
# );


def _first(row, *keys):
    """Ambil nilai pertama yang terisi dari beberapa kemungkinan nama field."""
    for key in keys:
        value = row.get(key)
        if value not in (None, ""):
            return value
    return None


def _to_int(value):
    """Ubah ke integer, kosong atau tidak valid menjadi NULL."""
    try:
        return int(value) if value not in (None, "") else None
    except (TypeError, ValueError):
        return None


def _to_date(value):
    """Ubah teks tanggal (YYYY-MM-DD) menjadi date, gagal menjadi NULL."""
    if not value:
        return None
    try:
        return datetime.strptime(str(value)[:10], "%Y-%m-%d").date()
    except ValueError:
        return None


@dag(
    dag_id="sister_visiting_scientist",
    schedule="0 3 * * *",  # 30 menit setelah sister_publikasi (UTC)
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=5)},
    tags=["sister", "etl", "mariadb"],
)
def sister_visiting_scientist():

    @task
    def tarik_dan_simpan_visiting_scientist():

        # 1. EXTRACT: login lalu GET /visiting_scientist
        data = get_json(ENDPOINT, {"id_sdm": ID_SDM})

        if not isinstance(data, list):
            raise ValueError("Format respons API tidak sesuai. Diharapkan JSON array.")

        total_data = len(data)
        log.info("Berhasil mengambil %d data visiting scientist dari SISTER.", total_data)
        if total_data == 0:
            log.info("Tidak ada data visiting scientist untuk disimpan.")
            return

        # 2. TRANSFORM: rapikan tipe data tiap baris
        rows = []
        total_skipped = 0
        for row in data:
            vs_id = row.get("id")
            if not vs_id:
                log.warning("Data visiting scientist dilewati karena tidak memiliki ID.")
                total_skipped += 1
                continue

            tanggal_raw = _first(row, "tanggal_pelaksanaan", "tanggal_mulai", "tanggal")
            tanggal = _to_date(tanggal_raw)
            if tanggal_raw and tanggal is None:
                log.warning("Format tanggal tidak dikenali (id=%s), disimpan NULL.", vs_id)

            rows.append((
                str(vs_id),
                ID_SDM,
                row.get("kategori_kegiatan"),
                _first(row, "perguruan_tinggi_pengundang", "nama_perguruan_tinggi", "perguruan_tinggi"),
                _to_int(row.get("lama_kegiatan")),
                tanggal,
                json.dumps(row, ensure_ascii=False),
            ))

        if not rows:
            log.warning("Tidak ada baris valid untuk disimpan.")
            return

        # 3. LOAD: simpan ke MariaDB dalam satu transaksi
        db = None
        try:
            db = get_db_connection()
            with db.cursor() as cursor:
                cursor.executemany(QUERY, rows)
            db.commit()
            log.info(
                "Proses ETL berhasil. Total dari API: %d | Disimpan: %d | Dilewati: %d",
                total_data, len(rows), total_skipped,
            )
        except Exception:
            if db:
                db.rollback()
            log.exception("Gagal menyimpan data visiting scientist ke MariaDB.")
            raise
        finally:
            if db:
                db.close()

    tarik_dan_simpan_visiting_scientist()


sister_visiting_scientist()
