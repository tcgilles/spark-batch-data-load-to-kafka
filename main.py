import sys
import uuid
from typing import Any

from lib import utils, config_loader
from lib.logger import Log4j

if __name__ == '__main__':

    if len(sys.argv) < 3:
        print("Usage: sbdl {local, qa, prod} {load_date} : Arguments are missing")
        sys.exit(-1)

    job_run_env: str = sys.argv[1].upper()
    load_date: str = sys.argv[2]
    job_run_id: str = f"SBDL-{str(uuid.uuid4())}"

    print("Initializing SBDL Job in " + job_run_env + " Job ID: " + job_run_id)
    conf: dict[str, Any] = config_loader.get_config(job_run_env)
    enable_hive: bool = True if conf["enable.hive"] == "true" else False
    hive_db: str = conf["hive.database"]

    print("Creating Spark Session")
    spark = utils.get_spark_session(job_run_env)
    logger = Log4j(spark)
    logger.info("Finished creating Spark Session")
