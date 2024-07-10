# ams-background-tasks
AMS Background tasks

## Airflow

Below is a brief explanation about Airflow instalation. See the complete documentation [here](https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html).

### Requirements

Docker Compose v2.14.0 or newer.

### Initializing environment

Before starting Airflow for the first time, you need to prepare your environment, i.e. create the necessary files, directories and initialize the database.

#### Setting the right Airflow user

On Linux, the quick-start needs to know your host user id and needs to have group id set to 0. Otherwise the files created in **dags**, **logs** and **plugins** will be created with root user ownership. You have to make sure to configure them for the docker-compose:

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