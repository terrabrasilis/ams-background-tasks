from airflow.models import Variable

from ams_background_tasks.tools.common import BIOMES


def get_biomes_from_variable():
    return " ".join(f"--biome={b}" for b in Variable.get("AMS_BIOMES").split(";") if b)


def get_all_biomes():
    return " ".join(f"--biome='{b}'" for b in BIOMES)
