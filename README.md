# ams-background-tasks

The **AMS Background Tasks** is a set of tools designed to create and update the database of the **Amazon Situation Room (AMS)**. The execution of these tools is managed by **Airflow**.

In short, **Airflow** is a platform created by the community to programmatically author, schedule, and monitor workflows or DAGs. In Airflow, a **DAG** (*Directed Acyclic Graph*) is a collection of tasks that you want to run, organized in a way that reflects their relationships and dependencies. A DAG is defined in a Python script, which represents the DAG's structure (tasks and their dependencies) as code.

The **DAG `ams-create-db`** is responsible for creating and updating the AMS database. This DAG consists of following tasks:

1. `check-variables`
2. `update-environment`
3. `check-recreate-db`
4. `create-db`
5. `update-biome`
6. `update-spatial-units`
7. `update-active-fires`
8. `update-amz-deter`
9. `update-cer-deter`
10. `download-risk-file`
11. `update-ibama-risk`
12. `prepare-classification`
13. `classify-deter-by-land-use`
14. `classify-fires-by-land-use`
15. `finalize-classification`

Each of these tasks is a Python command-line tool developed using the **Click** library.

To run the DAG `ams-create-db`, three external databases are required: one for **DETER data** (for each biome), another for **active fires data**, and an **auxiliary database**.

From the auxiliary database, the following tables are required:

- `public.lm_bioma_250`
- `public.municipio_test`
- `public.lml_unidade_federacao_a`
- `cs_amz_5km`
- `cs_amz_5km_biome`
- `cs_amz_25km`
- `cs_amz_25km_biome`
- `cs_amz_150km`
- `cs_amz_150km_biome`
- `cs_cer_5km`
- `cs_cer_5km_biome`
- `cs_cer_25km`
- `cs_cer_25km_biome`
- `cs_cer_150km`
- `cs_cer_150km_biome`

The cell tables (prefixed with `cs_`), except for the 5km ones, are created by the notebook [`update_auxiliary.ipynb`](https://github.com/terrabrasilis/ams-background-tasks/blob/main/notebooks/update_auxiliary.ipynb), which uses data from the existing AMS Database. The tables `cs_*_5km*`, however, are created by the notebook [`import_cells_from_shapefile.ipynb`](https://github.com/terrabrasilis/ams-background-tasks/blob/main/notebooks/import_cells_from_shapefile.ipynb), which allows for importing cells into the auxiliary database from a shapefile. If the cell tables are not defined in the auxiliary database, it is necessary to run these notebooks. The shapefile containing the 5km cells is attached to [`issue #26`](https://github.com/terrabrasilis/ams-background-tasks/issues/26).

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
1) `AMS_AF_DB_URL` (Raw fires database, ex: raw_fires_data)
2) `AMS_AUX_DB_URL` (Auxiliary database, ex: auxiliary)
3) `AMS_AMZ_DETER_B_DB_URL` (Deter Amazonia database, ex: deter_amazonia_nb)
4) `AMS_CER_DETER_B_DB_URL` (Deter Cerrado database, ex: deter_amazonia_nb)
5) `AMS_DB_URL` (AMS ouput database, ex: ams_new)
6) `AMS_FTP_URL` (FTP to get the risk file provided by IBAMA)


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

1) `AIRFLOW_UID`: see [Setting the right Airflow user](https://github.com/terrabrasilis/ams-background-tasks?tab=readme-ov-file#setting-the-right-airflow-user). Example: AIRFLOW_UID=1000
2) `AMS_FORCE_RECREATE_DB`: the expected values are 0 or 1. When enabled, it forces the recreation of the AMS database. Example: AMS_FORCE_RECREATE_DB=1
3) `AMS_ALL_DATA_DB`: the expected values are 0 or 1. When enabled, it updates all data, including historical data. Example: AMS_ALL_DATA_DB=1
4) `AMS_BIOMES`: a list of biomes separated by semicolons. Example: AMS_BIOMES="AmazÃ´nia;Cerrado;".
5) `AMS_STAC_API_URL`: the STAC API url to retrieve the risk image. Example: AMS_STAC_API_URL=https://terrabrasilis.dpi.inpe.br/stac-api/v1/
6) `AMS_STAC_COLLECTION`: the STAC collection name. Example: AMS_STAC_COLLECTION=collection1

#### Files and Volumes

Additionally, it is necessary to place the land use files in the `land_use` directory. The naming convention for the files is: `{BIOMA}_land_use`.tif (e.g., `Amazônia_land_use.tif`, `Cerrado_land_use.tif`, and so on).
