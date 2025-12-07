# MPlus Software Data Engineering Pipeline

## Deskripsi Proyek
Proyek ini merupakan pipeline data engineering untuk memproses data transaksi MPlus Software.  
Pipeline dibangun menggunakan **Docker**, **Airflow**, **dbt**, dan Python, serta mengikuti framework **layered architecture Postgre database**:  

**Staging → Refined → Datamart**  

Pipeline menangani ekstraksi data, transformasi, dan pemuatan ke database serta mendukung integrasi dengan dbt.


## Prerequisites

Sebelum menjalankan pipeline, pastikan lingkungan pengembangan sudah memenuhi:

Software:
- Docker Desktop (Engine): v20.10.x+
- Docker Compose: v1.29.x+
- Git

Komponen Internal (Sudah terkunci docker):
- Apache Airflow: v2.9.0
- PostgreSQL: v15

Cek versi (CMD):
python --version
docker --version
docker-compose --version
psql --version


## Akses Layanan

1. Airflow Webserver (UI)
    URL: http://localhost:8080
    Username: admin
    Password: admin

2. Flower (Monitoring Celery)
    URL: http://localhost:5555

3. PostgreSQL Database
    Host: localhost
    Port: 5432
    User/Password: admin/admin
    Database: 
    - dwh: Data Warehouse (penyimpanan untuk schema refine dan dmart)
    - airflow: Metadata


## Data Architecture

1. Staging
Tempat data mentah dimuat dari source, penyimpanan berupa bucket dalam format parquet.
    Contoh tabel: staging/mplus_stt.parquet
Data disimpan apa adanya di folder Staging, belum dibersihkan.

2. Refined
Data dibersihkan, normalisasi dasar, duplikasi dihapus, missing value di-handle.
    Folder script: script/refine

3. Datamart
Layer akhir untuk reporting dan dashboard.
    Folder script: script/dmart
Di sini data sudah ditransformasi sesuai kebutuhan analisis.


## Struktur Folder 
```
.
├─ .venv/                  # Virtual environment Python
├─ airflow/                
│  ├─ config/              # Konfigurasi Airflow (env, connections, variables)
│  ├─ dags/                # Folder untuk DAG Airflow
│  │  ├─ dmart/            # DAG untuk proses Datamart
│  │  └─ source_to_target/ # DAG untuk pipeline ETL dari source ke target
│  ├─ logs/                # Log eksekusi Airflow
│  └─ plugins/             # Custom operator, sensor, hook Airflow
├─ config/                 
│  ├─ config/              # Konfigurasi tambahan proyek
│  ├─ dags/                # Konfigurasi DAG jika perlu di luar airflow/dags
│  ├─ logs/                # Log tambahan jika ada
│  └─ plugins/             # Plugin tambahan jika ada
│  └─ docker-compose.yaml  # File docker-compose untuk menjalankan Airflow & dependencies
├─ logs/                   # Log umum pipeline/skrip di luar Airflow
├─ script/                 
│  ├─ dbt/                 # Script dbt models atau run command
│  ├─ dmart/               # Script transformasi data untuk datamart
│  ├─ refine/              # Script untuk data refining/cleaning
│  └─ staging/             # Script untuk load data ke staging
├─ staging/                
│  └─ mplus_stt.parquet    # Penyimpanan Bucket Data staging
├─ README.md               # Dokumentasi proyek
└─ requirements.txt        # List dependency Python
```


## Clone Repository
git clone https://github.com/username/MPlus_Software.git
cd MPlus_Software


## Integrasi Airflow

1. Jalankan container Airflow:

``` 
# Build image pertama kali (tanpa cache)
docker compose build --no-cache --progress=plain

# Start Container
docker-compose up -d 

# Cek Status Container
docker compose ps
```

2. Akses Airflow Web UI: http://localhost:8080

3. Trigger DAG sesuai pipeline di folder:
```
airflow/dags/source_to_target
airflow/dags/dmart
```

Testing & Debugging

- Cek container Docker:
```
docker ps
```

- Masuk ke container untuk debug:
```
docker exec -it <container_id> /bin/bash
```

- Cek log Python:
```
tail -f logs/<nama_script>.log
```


## Install Dependencies Requirements.txt

Jalankan perintah berikut:
```
pip install -r requirements.txt
```

## Catatan Penting & Tips Troubleshooting

- Pastikan Docker Compose berjalan sebelum menjalankan script Python.
- Periksa environment variable dan port mapping PostgreSQL jika koneksi gagal.
- Selalu update last_status di source table sebelum pipeline dijalankan.
- Gunakan virtual environment untuk menghindari konflik package.



