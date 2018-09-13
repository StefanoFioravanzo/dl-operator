import settings.settings as settings
from controller import DLOperator

import logging
from logging.config import fileConfig

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger()
fileConfig("logging.ini")

if __name__ == "__main__":
    logging.getLogger("kubernetes").setLevel(logging.CRITICAL)
    logger.info("Creating Controller...")
    controller = DLOperator()
    controller.create_crd(crd_path=settings.CRD)
    # controller.create_dljob()
    controller.watch_crd()