# 1. Install dbt-core + dbt-postgres

Di PowerShell:
pip install dbt-postgres

Cek versi:
dbt --version

Jika muncul:
  Core:
    - installed: 1.10.x
  postgres: OK
Berarti instalasi berhasil.

# 2. Buat folder project dbt di script\dbt

Arahkan terminal ke folder script:
cd script

Lalu buat project dbt di dalamnya:
dbt init dbt


Setelah perintah ini, dbt akan menanyakan:
Nama project → dbt
Jenis database → pilih postgres
Host → localhost

Selesai → maka folder terbentuk:
script\dbt\

# 3. Buat file profiles.yml

dbt tidak otomatis membuat profiles.yml di dalam project.
Kita perlu membuatnya secara manual di lokasi project:
script\dbt\profiles.yml

Isi file tersebut:

dbt:
  target: dev
  outputs:
    dev:
      type: postgres
      host: localhost
      user: admin
      password: admin
      port: 5432
      dbname: dwh
      schema: refine

Pastikan indentasi 2 spasi, bukan tab.

# 4. Test koneksi dbt

Masuk ke folder proyek:
cd script\dbt

Tes koneksi:
dbt debug


Jika berhasil, muncul:
Connection test: OK connection ok

Artinya dbt sudah terhubung ke PostgreSQL DWH