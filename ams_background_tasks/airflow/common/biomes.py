from airflow.models import Variable

from ams_background_tasks.airflow.common.vars import VAR_BIOMES
from ams_background_tasks.tools.common import BIOMES


def get_biomes_from_variable():
    return " ".join(f"--biome={b}" for b in Variable.get(VAR_BIOMES).split(";") if b)


def get_all_biomes():
    return " ".join(f"--biome='{b}'" for b in BIOMES)
