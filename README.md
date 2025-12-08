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
    ```
    URL: http://localhost:8080
    ```
    Username: admin
    Password: admin

3. Flower (Monitoring Celery)
    ```
    URL: http://localhost:5555
    ```

5. PostgreSQL Database
    Host: localhost  
    Port: 5432  
    Username: admin  
    Password: admin  
    Database: 
    - dwh: Data Warehouse (penyimpanan untuk schema refine dan dmart)
    - airflow: Metadata


## Data Architecture

1. Staging
Tempat data mentah dimuat dari source, penyimpanan berupa bucket dalam format parquet.
```
   Contoh tabel: staging/mplus_stt.parquet
```
Data disimpan apa adanya di folder Staging, belum dibersihkan.

3. Refined
Data dibersihkan, normalisasi dasar, duplikasi dihapus, missing value di-handle.
```
Folder script: script/refine
```

5. Datamart
Layer akhir untuk reporting dan dashboard.
```
Folder script: script/dmart
```
Di sini data sudah ditransformasi sesuai kebutuhan analisis.


## Data Flow
Data flow ini mendeskripsikan pergerakan data dari sumber eksternal hingga menjadi Data Mart di Data Warehouse (PostgreSQL):
- Extract (E): Airflow Worker ambil data mentah dari Google Drive / SFTP / API / file lokal.
- Load Staging (L): Data dimuat ke skema Staging di lokal dalam bentuk parquet.
- Transform Refined (T): Airflow panggil dbt untuk pembersihan dan standarisasi di skema Refined, serta dbt membangun tabel Data Mart.
- Load Data Mart (L): Data hasil transformasi masuk ke skema Dmart (misal: dm_mplus_billing) untuk dashboard & BI.
- Logging & Metadata: Airflow simpan status DAG, log eksekusi, dan metadata di DB dan folder ./logs.


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
│  └─ docker-compose.yaml  # File docker-compose untuk menjalankan Airflow & dependencies
│  └─ dockerfile           # List dependensi Python yang diperlukan
│  └─ requirements.txt     # Instruksi untuk membangun image Docker
├─ script/                 
│  ├─ dbt/                 # Script dbt models atau run command
│  ├─ dmart/               # Script transformasi data untuk datamart
│  ├─ refine/              # Script untuk data refining/cleaning
│  └─ staging/             # Script untuk load data ke staging
├─ staging/                
│  └─ mplus_stt.parquet    # Penyimpanan Bucket Data staging
├─ README.md               # Dokumentasi proyek
```

## How-to Set Up

### 1. Clone Repository
```
git clone https://github.com/username/MPlus_Software.git 
```


### 2. Integrasi Airflow

1. Jalankan container Airflow:

```
# Change directory ke folder config
cd MPlus_Software\config

# Build image pertama kali
docker compose up --build -d

# Start Container
docker-compose up -d 

# Cek Status Container
docker compose ps
```

2. Akses Airflow Web UI
```
http://localhost:8080
```
   Username: admin
   Password: admin

4. Trigger DAG sesuai pipeline di folder:
```
airflow/dags/source_to_target
airflow/dags/dmart
```

5. Testing & Debugging

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


### 3. Menjalankan DAG

Prosedur ini mengasumsikan semua layanan Docker Airflow Anda sudah berjalan (docker compose up -d).
- Akses UI: Buka Airflow Webserver di http://localhost:8080 dan Login (admin/admin).
- Cari DAG: Navigasi ke DAGs View (Halaman Utama) dan cari dm_mplus_billing.
- Aktifkan (Unpause): Klik toggle switch di baris DAG untuk mengubah status dari Paused (Abu-abu) menjadi Aktif (Hijau).
- Trigger Manual: Klik tombol ▶ Trigger DAG (ikon play) untuk memicu eksekusi manual.
- Monitor: Klik pada nama DAG, lalu gunakan Graph View untuk memantau status setiap tugas (task) yang berjalan.
- Cek Log: Jika ada tugas yang gagal, klik tugas tersebut di Graph View dan pilih menu Log untuk debugging.

## Catatan Penting & Tips Troubleshooting

- Pastikan Docker Compose berjalan sebelum menjalankan script Python.
- Periksa environment variable dan port mapping PostgreSQL jika koneksi gagal.
- Selalu update last_status di source table sebelum pipeline dijalankan.
- Gunakan virtual environment untuk menghindari konflik package.



