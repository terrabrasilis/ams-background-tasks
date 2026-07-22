# ams-background-tasks

`ams-background-tasks` is a Python library with background processing tools for the **Amazon Situation Room (AMS)**.

Execution is orchestrated by **Airflow**. Each DAG defines a sequence of tasks, and each task runs a command-line tool from the library, usually implemented with `click`.

## Overview

The package includes DAGs for:

- creating and updating the AMS database
- updating biome and spatial unit tables
- updating indicators based on DETER, active fires, deforestation risk, fire spreading risk, and PRODES
- preparing and finalizing the tables used in fundiary classifications
- calculating area by fundiary classification
- sending a processing summary by email

## Implemented DAGs

The DAGs available in `ams_background_tasks/airflow/dags` are:

- `ams-create-db`
- `ams-process-fire-spreading-risk-file`
- `ams-calculate-land-use-area`
- `ams-update-prodes`
- `ams-finalize-prodes`

### 1. `ams-create-db`

Responsible for creating, preparing, and updating the main AMS environment.

Schedule: `15 * * * *`

#### Task flow

| Task ID | What it does | Tool / callable |
| --- | --- | --- |
| `check-variables` | Validates required variables and connections | `check_variables` |
| `update-environment` | Creates/updates the virtualenv and installs the library | `update_environment` |
| `check-create-db` | Decides whether the database should be recreated | `check_recreate_db` |
| `create-db` | Creates the AMS database | `ams-create-db` |
| `skip-create-db` | Skip branch | `EmptyOperator` |
| `update-biome` | Updates the biome table | `ams-update-biome` |
| `update-spatial-units` | Updates spatial units | `ams-update-spatial-units` |
| `prepare-classification-ams` | Prepares tables for AMS classification | `ams-prepare-classification --land-use-type ams` |
| `prepare-classification-ppcdam` | Prepares tables for PPCDAM classification | `ams-prepare-classification --land-use-type ppcdam` |
| `prepare-classification-prodes` | Prepares tables for PRODES classification | `ams-prepare-classification --land-use-type prodes` |
| `need-update-fires` | Checks whether active fires should be updated | `ams-need-update-indicator --indicator=focos` |
| `decide-update-fires` | Branches to update or skip | `decide_update_fires` |
| `update-active-fires` | Updates active fires | `ams-update-active-fires` |
| `skip-update-fires` | Skip branch | `EmptyOperator` |
| `need-update-fires-today` | Checks whether today active fires should be updated | `ams-need-update-indicator --indicator=focos-hoje` |
| `decide-update-fires-today` | Branches to update or skip | `decide_update_fires_today` |
| `update-active-fires-today` | Updates today active fires | `ams-update-active-fires-today` |
| `skip-update-fires-today` | Skip branch | `EmptyOperator` |
| `need-update-deter` | Checks whether DETER should be updated | `ams-need-update-indicator --indicator=deter` |
| `decide-update-deter` | Branches to update or skip | `decide_update_deter` |
| `update-amz-deter` | Updates DETER for Amazonia | `ams-update-deter --biome='Amazônia'` |
| `update-cer-deter` | Updates DETER for Cerrado | `ams-update-deter --biome='Cerrado'` |
| `update-pan-deter` | Updates DETER for Pantanal | `ams-update-deter --biome='Pantanal'` |
| `skip-update-deter` | Skip branch | `EmptyOperator` |
| `finalize-deter-update` | Finalizes DETER consolidation | `ams-finalize-deter-update` |
| `need-update-risk` | Checks whether INPE risk should be updated | `ams-need-update-indicator --indicator=risco` |
| `decide-update-risk` | Branches to update or skip | `decide_update_risk` |
| `download-inpe-risk-file` | Downloads the INPE risk file | `ams-download-inpe-risk-file` |
| `update-inpe-risk` | Updates the INPE risk base | `ams-update-inpe-risk` |
| `skip-update-risk` | Skip branch | `EmptyOperator` |
| `need-update-fire-sr` | Checks whether fire spreading risk should be updated | `ams-need-update-indicator --indicator=risco-espalhamento-fogo` |
| `decide-update-fire-sr` | Branches to update or skip | `decide_update_fire_sr` |
| `import-fire-sr-file` | Imports the fire spreading risk file | `ams-import-fire-spreading-risk-file` |
| `update-fire-sr` | Processes the fire spreading risk file | `ams-process-fire-spreading-risk-file` |
| `skip-update-fire-sr` | Skip branch | `EmptyOperator` |
| `classify-fires-by-land-use-ams` | Classifies fires by fundiary classification for AMS | `ams-classify-by-land-use --indicator='focos' --land-use-type=ams` |
| `classify-fires-by-land-use-ppcdam` | Classifies fires by fundiary classification for PPCDAM | `ams-classify-by-land-use --indicator='focos' --land-use-type=ppcdam` |
| `classify-fires-by-land-use-prodes` | Classifies fires by fundiary classification for PRODES | `ams-classify-by-land-use --indicator='focos' --land-use-type=prodes` |
| `classify-fires-today-by-land-use-ams` | Classifies today fires by fundiary classification for AMS | `ams-classify-by-land-use --indicator='focos-hoje' --land-use-type=ams` |
| `classify-fires-today-by-land-use-ppcdam` | Classifies today fires by fundiary classification for PPCDAM | `ams-classify-by-land-use --indicator='focos-hoje' --land-use-type=ppcdam` |
| `classify-deter-by-land-use-ams` | Classifies DETER by fundiary classification for AMS | `ams-classify-by-land-use --indicator='deter' --land-use-type=ams` |
| `classify-deter-by-land-use-ppcdam` | Classifies DETER by fundiary classification for PPCDAM | `ams-classify-by-land-use --indicator='deter' --land-use-type=ppcdam` |
| `classify-risk-by-land-use-ams` | Classifies INPE risk by fundiary classification for AMS | `ams-classify-by-land-use --indicator='risco' --land-use-type=ams` |
| `classify-risk-by-land-use-ppcdam` | Classifies INPE risk by fundiary classification for PPCDAM | `ams-classify-by-land-use --indicator='risco' --land-use-type=ppcdam` |
| `classify-fire-sr-by-land-use-ams` | Classifies fire spreading risk by fundiary classification for AMS | `ams-classify-by-land-use --indicator='risco-espalhamento-fogo' --land-use-type=ams` |
| `classify-fire-sr-by-land-use-ppcdam` | Classifies fire spreading risk by fundiary classification for PPCDAM | `ams-classify-by-land-use --indicator='risco-espalhamento-fogo' --land-use-type=ppcdam` |
| `finalize-classification-ams` | Finalizes AMS classification | `ams-finalize-classification --land-use-type=ams` |
| `finalize-classification-ppcdam` | Finalizes PPCDAM classification | `ams-finalize-classification --land-use-type=ppcdam` |
| `finalize-classification-prodes` | Finalizes PRODES classification | `ams-finalize-classification --land-use-type=prodes` |
| `drop-temp-tables` | Drops temporary tables | `ams-drop-temp-tables` |
| `retrieve-process-status` | Collects process summary | `ams-print-process-status` |
| `prepare-status-email` | Prepares the email subject and body | `prepare_status_email` |
| `send-status-email` | Sends the status email | `EmailOperator` |

### 2. `ams-update-prodes`

Builds PRODES indicators from the PRODES TIFF using the annual deforestation increments available since the beginning of the historical series.

The input TIFFs must be stored in the directory provided by `--prodes-root-dir`, using the naming convention `prodes_{biome}.tif` for each biome. For example, `prodes_Caatinga.tif`.

During execution, the tool creates a cache structure under `--prodes-root-dir/cache/{biome}/{land_use_type}` and splits the raster into chunks. Each chunk is processed independently.

The generated datasets are kept as pandas/GeoPandas tabular outputs and cached as pickle files. When `--save-indicators` is enabled, the generated indicators are also persisted into the land-use tables under the `prodes` schema.

Schedule: `None`

Tool execution example:

```bash
ams-update-prodes --biome Amazônia --years 2000 2025 --land-use-dir /home/marcosilva/terrabrasilis/land_use/new/ --land-use-type ams --prodes-root-dir /home/marcosilva/terrabrasilis/prodes/ --chunk-size 1000 --save-indicators --force-recreate
```

This example calculates the PRODES deforestation indicators for the 2000 to 2025 historical interval. The generated data is classified using the AMS fundiary categories, the raster is split into `1000 x 1000` pixel chunks, and the database is recreated before processing.

| Task ID | What it does | Tool / callable |
| --- | --- | --- |
| `check-variables` | Validates required variables and connections | `check_variables` |
| `update-environment` | Creates/updates the virtualenv and installs the library | `update_environment` |
| `update-prodes-ams` | Updates PRODES indicators for AMS | `ams-update-prodes --land-use-type=ams` |
| `update-prodes-ppcdam` | Updates PRODES indicators for PPCDAM | `ams-update-prodes --land-use-type=ppcdam` |

### 3. `ams-process-fire-spreading-risk-file`

Downloads, processes, and prepares the fire spreading risk file.

Schedule: `0 2 * * *`

| Task ID | What it does | Tool / callable |
| --- | --- | --- |
| `check-variables` | Validates required variables and connections | `check_variables` |
| `update-environment` | Creates/updates the virtualenv and installs the library | `update_environment` |
| `download-fire-sr-file` | Downloads the fire spreading risk file | `ams-download-fire-spreading-risk-file` |
| `process-fire-sr-file` | Processes the downloaded file | `ams-process-fire-spreading-risk-file` |

### 4. `ams-calculate-land-use-area`

Calculates the area of each fundiary category across all spatial units. For every spatial unit, the exact land-use coverage can be derived, producing the tables with the `land_use_area` suffix. These tables are required to render the profile in the AMS WebGIS, because they expose the land-use coverage of the spatial unit being analyzed.

This DAG should be executed only once. After the tables are created, recalculation is not necessary because the underlying data is static.

Schedule: `None`

| Task ID | What it does | Tool / callable |
| --- | --- | --- |
| `check-variables` | Validates required variables and connections | `check_variables` |
| `update-environment` | Creates/updates the virtualenv and installs the library | `update_environment` |
| `calculate-biomes-land-use-area` | Calculates land-use coverage by fundiary category and spatial unit | `ams-calculate-land-use-area` |

### 5. `ams-finalize-prodes`

Copies the land-use data stored in the `prodes` schema into the AMS land-use tables in the `public` schema.

Schedule: `None`

| Task ID | What it does | Tool / callable |
| --- | --- | --- |
| `check-variables` | Validates required variables and connections | `check_variables` |
| `update-environment` | Creates/updates the virtualenv and installs the library | `update_environment` |
| `finalize-prodes-ams` | Copies PRODES land-use data into AMS tables | `ams-finalize-prodes --land-use-type=ams` |
| `finalize-prodes-ppcdam` | Copies PRODES land-use data into PPCDAM tables | `ams-finalize-prodes --land-use-type=ppcdam` |

## Fundiary classification bases

The classification tasks use two different fundiary classification bases:

- `AMS`: consolidated fundiary categories
- `PPCDAM`: fundiary categories with CAR overlap distinction

The category names below are intentionally kept in Portuguese, since they match the business terminology used by the project.

### AMS categories

| Code | Category |
| --- | --- |
| `1` | Terra indígena |
| `2` | Unidade de Conservação de Proteção Inegral |
| `3` | Unidade de conservacão de Uso Sustentável (sem APA) |
| `4` | Território Quilombola |
| `5` | Assentamento Rural |
| `6` | Área de Proteção Ambiental |
| `7` | Propriedade Privada (Dados do SIGEF) |
| `8` | Floresta pública não destinada |
| `9` | Área Sem Registro Fundiário |

### PPCDAM categories

| Code | Category |
| --- | --- |
| `1` | Terra Indígena |
| `2` | Unidade de Conservação |
| `3` | Território Quilombola |
| `4` | Assentamento Rural |
| `5` | Área de Proteção Ambiental |
| `6` | Floresta Pública Não Destinada |
| `7` | CAR sobreposto em Terra Indígena |
| `8` | CAR sobreposto em Unidade de Conservação |
| `9` | CAR sobreposto em Território Quilombola |
| `10` | CAR sobreposto em Assentamento Rural |
| `11` | CAR sobreposto em Área de Proteção Ambiental |
| `12` | CAR sobreposto em Floresta Pública Não Destinada |
| `13` | Propriedade Privada (Dados do CAR) |
| `14` | Área Sem Registro Fundiário |

## Environment variables and configuration used by the tools

It helps to separate three groups:

1. Airflow connections
2. Airflow variables
3. Environment variables read directly by the CLI tools

### Airflow connections

The DAGs validate these connections:

- `AMS_DB_URL`
- `AMS_AUX_DB_URL`
- `AMS_AF_DB_URL`
- `AMS_FC_DB_URL`
- `AMS_AMZ_DETER_B_DB_URL`
- `AMS_CER_DETER_B_DB_URL`
- `AMS_PAN_DETER_B_DB_URL`

Important note:

- `AMS_AMZ_DETER_B_DB_URL`, `AMS_CER_DETER_B_DB_URL`, and `AMS_PAN_DETER_B_DB_URL` are converted into an environment variable called `AMS_DETER_B_DB_URL` inside the DETER tasks.

### Databases used

| Connection / identifier | Database name |
| --- | --- |
| `AMS_DB_URL` | ams3 (production) and ams2_homologation (homologation) |
| `AMS_AUX_DB_URL` | auxiliary |
| `AMS_AF_DB_URL` | raw_fires_data  |
| `AMS_FC_DB_URL` | fires_dashboard |
| `AMS_AMZ_DETER_B_DB_URL` | deter_amazonia_nb  |
| `AMS_CER_DETER_B_DB_URL` | deter_cerrado_nb |
| `AMS_PAN_DETER_B_DB_URL` | deter_pantanal_nb  |

### Airflow variables

The following variables are checked by `check-variables` and used by the DAG flow:

| Variable | Expected value | Purpose |
| --- | --- | --- |
| `AMS_ALL_DATA_DB` | `0` or `1` | Enables processing with full historical data. |
| `AMS_FORCE_RECREATE_DB` | `0` or `1` | Forces database and table recreation. |
| `AMS_BIOMES` | Semicolon-separated biome list, for example `Amazônia;Cerrado` | Used by library helpers to build biome filters. |
| `AMS_STAC_API_URL` | STAC API URL | Used to download the INPE risk image. |
| `AMS_STAC_COLLECTION` | STAC collection name | Used to download the INPE risk image. |
| `AMS_EMAIL_TO` | Email address or list of addresses | Destination for the status email. |
| `AMS_FREQUENCY_TO_UPDATE_DETER` | Integer, in seconds | Controls DETER update frequency. |
| `AMS_FREQUENCY_TO_UPDATE_FIRES` | Integer, in seconds | Controls active fires update frequency. |
| `AMS_FREQUENCY_TO_UPDATE_FIRES_TODAY` | Integer, in seconds | Controls today active fires update frequency. |
| `AMS_FREQUENCY_TO_UPDATE_RISK` | Integer, in seconds | Controls INPE risk update frequency. |
| `AMS_FREQUENCY_TO_UPDATE_FIRE_SR` | Integer, in seconds | Controls fire spreading risk update frequency. |
| `AMS_FORCE_UPDATE_AT` | Integer from `0` to `23` | UTC hour after which a forced update is allowed. |
| `AMS_LIMIT` | Integer | Limits the number of processed records for testing. |

### Environment variables consumed by the CLI tools

The library tools read these environment variables directly:

- `AMS_DB_URL`
- `AMS_AUX_DB_URL`
- `AMS_AF_DB_URL`
- `AMS_FC_DB_URL`
- `AMS_DETER_B_DB_URL`
- `AMS_STAC_API_URL`
- `AMS_STAC_COLLECTION`
- `AMS_FTP_URL`

Main uses:

- `AMS_DB_URL`: main AMS database connection
- `AMS_AUX_DB_URL`: auxiliary database connection
- `AMS_AF_DB_URL`: active fires database connection
- `AMS_FC_DB_URL`: fires dashboard database connection
- `AMS_DETER_B_DB_URL`: DETER database connection for the current biome
- `AMS_STAC_API_URL` and `AMS_STAC_COLLECTION`: INPE risk download
- `AMS_FTP_URL`: IBAMA risk download

## Runtime files and directories

Some tools write files into local project directories:

- `land_use/`
- `risk/`
- `fire_spreading_risk/`
- `prodes/`

For the land-use coverage calculation flow, the expected files are:

- `land_use/ams/land_use.tif`
- `land_use/ppcdam/land_use.tif`

## Data dependencies

For `ams-create-db`, the library expects:

- a main AMS database
- an auxiliary database
- one DETER database per used biome
- an active fires database
- a fires dashboard database

The following tables must exist in the auxiliary database:

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

## How environment setup works

The DAGs call `update-environment`, which:

1. creates a virtualenv in `/tmp/venvs/<project-name>` when needed
2. activates that environment
3. installs the library itself with `pip install <project>`

## Operational notes

- `ams-create-db` runs `check-variables` before any other step.
- Update tasks use branching to skip steps when the schedule does not require processing.
- Classification tasks use `ams-classify-by-land-use` with different indicators and `land-use-type` values.
- The PRODES flow uses local directories for cache, chunking, reprojection, and counting.

## Initial Airflow setup

Example minimum setup for running the DAGs in an Airflow environment:

### Connections

Configure the following connection IDs:

1. `AMS_AF_DB_URL`
2. `AMS_AUX_DB_URL`
3. `AMS_AMZ_DETER_B_DB_URL`
4. `AMS_CER_DETER_B_DB_URL`
5. `AMS_DB_URL`
6. `AMS_FTP_URL`
7. `smtp_default`
8. `AMS_PAN_DETER_B_DB_URL`
9. `AMS_FC_DB_URL`

### Variables

Configure the following variables:

1. `AIRFLOW_UID`
2. `AMS_FORCE_RECREATE_DB`
3. `AMS_ALL_DATA_DB`
4. `AMS_BIOMES`
5. `AMS_STAC_API_URL`
6. `AMS_STAC_COLLECTION`
7. `AMS_FREQUENCY_TO_UPDATE_DETER`
8. `AMS_FREQUENCY_TO_UPDATE_FIRES`
9. `AMS_FREQUENCY_TO_UPDATE_RISK`
10. `AMS_FREQUENCY_TO_UPDATE_FIRE_SR`
11. `AMS_LIMIT`
12. `AMS_EMAIL_TO`
