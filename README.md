# ams-background-tasks

The **AMS Background Tasks** is a set of tools designed to create and update the database of the **Amazon Situation Room (AMS)**. The execution of these tools is managed by **Airflow**.

In short, **Airflow** is a platform created by the community to programmatically author, schedule, and monitor workflows or DAGs. In Airflow, a **DAG** (*Directed Acyclic Graph*) is a collection of tasks that you want to run, organized in a way that reflects their relationships and dependencies. A DAG is defined in a Python script, which represents the DAG's structure (tasks and their dependencies) as code.

The **DAG `ams-create-db`** is responsible for creating and updating the AMS database. This DAG consists of following tasks:

1. `check-recreate-db`
2. `create-db`
3. `update-biome`
4. `update-spatial-units`
5. `update-active-fires`
6. `update-amz-deter`
7. `update-cer-deter`
8. `classify-deter-by-land-use`
9. `classify-fires-by-land-use`
10. `finalize-classification`

Each of these tasks is a Python command-line tool developed using the **Click** library.

To run the DAG `ams-create-db`, three external databases are required: one for **DETER data** (for each biome), another for **active fires data**, and an **auxiliary database**.

From the auxiliary database, the following tables are required:

- `public.lm_bioma_250`
- `public.municipio_test`
- `public.lml_unidade_federacao_a`
- `cs_amz_25km`
- `cs_amz_25km_biome`
- `cs_amz_150km`
- `cs_amz_150km_biome`
- `cs_cer_25km`
- `cs_cer_25km_biome`
- `cs_cer_150km`
- `cs_cer_150km_biome`

These cell tables (starting with `cs_`) are created by the notebook [`update_auxiliary.ipynb`](https://github.com/terrabrasilis/ams-background-tasks/blob/main/notebooks/update_auxiliary.ipynb), which uses data from the existing AMS Database.

To run the environment, you need to update the `secrets.sh` and `.env` files with real values.

From the .env file, the following variables must be defined:

- `AIRFLOW_UID`: see [Setting the right Airflow user](https://github.com/terrabrasilis/ams-background-tasks?tab=readme-ov-file#setting-the-right-airflow-user). Example: AIRFLOW_UID=1000
- `AMS_FORCE_RECREATE_DB`: the expected values are 0 or 1. When enabled, it forces the recreation of the AMS database. Example: AMS_FORCE_RECREATE_DB=1
- `AMS_ALL_DATA_DB`: the expected values are 0 or 1. When enabled, it updates all data, including historical data. Example: AMS_ALL_DATA_DB=1
- `AMS_BIOMES`: a list of biomes separated by semicolons. Example: AMS_BIOMES="AmazÃ´nia;Cerrado;".
- `_AIRFLOW_WWW_USER_USERNAME`: the airflow username. Example: _AIRFLOW_WWW_USER_USERNAME=airflow
- `_AIRFLOW_WWW_USER_PASSWORD`: the airflow password. Example: _AIRFLOW_WWW_USER_PASSWORD=airflow

The secrets.sh file defines the databases access information.

- `AMS Database`: echo "postgresql://ams:postgres@192.168.0.51:5432/AMS" > secrets/ams_db_url.txt
- `Auxiliary Database`: echo "postgresql://ams:postgres@192.168.0.51:5432/auxiliary" > secrets/ams_aux_db_url.txt
- `Active Fires Database`: echo "postgresql://ams:postgres@192.168.0.51:5432/raw_active_fires2" > secrets/ams_af_db_url.txt
- `Deter Database - Amazônia`: echo "postgresql://ams:postgres@192.168.0.51:5432/DETER-B" > secrets/ams_amz_deter_b_db_url.txt
- `Deter Database - Cerrado`: echo "postgresql://ams:postgres@192.168.0.51:5432/deter_cerrado_nb" > secrets/ams_cer_deter_b_db_url.txt

Additionally, it is necessary to place the land use files in the `land_use` directory. The naming convention for the files is: `{BIOMA}_land_use`.tif (e.g., `Amazônia_land_use.tif`, `Cerrado_land_use.tif`, and so on).

To verify that everything is working properly locally, run the command `make install`. This will install the necessary dependencies and check the Python version. Run the command `./secrets.sh` to create or update the secret variables.

To prepare Airflow, follow the steps described in the next section.

## Airflow

Below is a brief explanation about Airflow instalation. See the complete documentation [here](https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html).

### Requirements

Docker Compose v2.14.0 or newer.

### Initializing environment

Before starting Airflow for the first time, you need to prepare your environment, i.e. create the necessary files, directories and initialize the database.

#### Setting the right Airflow user

On Linux, the quick-start needs to know your host user id and needs to have group id set to 0. Otherwise the files created in *dags*, *logs* and *plugins* will be created with root user ownership. You have to make sure to configure them for the docker-compose:

```bash
$ echo -e "AIRFLOW_UID=$(id -u)" > .env
```

#### Initialize the database

You need to run database migrations and create the first user account. To do this, run.

```bash
$ docker compose up airflow-init
```

#### Running

Now you can start all services:

```bash
$ docker compose up
```

#### Cleaning-up the environment

The docker-compose environment we have prepared is a “quick-start” one. It was not designed to be used in production and it has a number of caveats - one of them being that the best way to recover from any problem is to clean it up and restart from scratch.

The best way to do this is to:

1. Run ```docker compose down --volumes --remove-orphans``` command in the directory you downloaded the *docker-compose.yaml* file.

2. Remove the entire directory where you downloaded the *docker-compose.yaml* file ```rm -rf '<DIRECTORY>'```.

3. Run through this guide from the very beginning, starting by re-downloading the *docker-compose.yaml* file.
