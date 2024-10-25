# ams-background-tasks

The **AMS Background Tasks** is a set of tools designed to create and update the database of the **Amazon Situation Room (AMS)**. The execution of these tools is managed by **Airflow**.

In short, **Airflow** is a platform created by the community to programmatically author, schedule, and monitor workflows or DAGs. In Airflow, a **DAG** (*Directed Acyclic Graph*) is a collection of tasks that you want to run, organized in a way that reflects their relationships and dependencies. A DAG is defined in a Python script, which represents the DAG's structure (tasks and their dependencies) as code.

The **DAG `ams-create-db`** is responsible for creating and updating the AMS database. This DAG consists of following tasks:

1. `check-variables`
2. `update-environment`
3. `check-recreate-db`
4. `check-recreate-db`
5. `create-db`
6. `update-biome`
7. `update-spatial-units`
8. `update-active-fires`
9. `update-amz-deter`
10. `update-cer-deter`
11. `classify-deter-by-land-use`
12. `classify-fires-by-land-use`
13. `finalize-classification`

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


```bash
$ jupyter-notebook notebooks/update_auxiliary.ipynb
```

## Run on Production Environment

This DAG is made to run from "DagBag", this means that all the dag files are inside the root folder.
Assuming that the Airflow environment is using 

### Initializing production airflow connections and variables

To run over an Airflow instance, it's necessary to setup the following airflow configurations:

#### Connections:

Setup this connections ids:
1)  AMS_AF_DB_URL (Raw fires database, ex: raw_fires_data)
2)  AMS_AUX_DB_URL (Auxiliary database, ex: auxiliary)
3)  AMS_AMZ_DETER_B_DB_URL (Deter Amazonia database, ex: deter_amazonia_nb)
4) AMS_CER_DETER_B_DB_URL (Deter Cerrado database, ex: deter_amazonia_nb)
5)  AMS_DB_URL (AMS ouput database, ex: ams_new)


Example how to setup the connection fields:

- Connection Id: AMS_AF_DB_URL (Id used by DAG)
- Connection Type: Postgres
- Host: 192.168.1.9 (Database host or IP)
- Database: raw_fires_data (Database name)
- Login: ams (Database username)
- Password: ams (Database password)
- Port: 5432 (Database port) 

#### Variables:

Setup the following variables:

1) AMS_ALL_DATA_DB (0 or 1)

2) AMS_BIOMES (Values separated by ;. ex: Amazônia;Cerrado;Pantanal)

3) AMS_FORCE_RECREATE_DB (0 or 1)


