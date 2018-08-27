import os
import settings.settings as settings
from controller import DLOperator

import logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(os.path.basename(__file__))

if __name__ == "__main__":
    logger.info("Creating Controller...")
    controller = DLOperator()
    controller.create_crd(crd_path=settings.CRD)
    # controller.create_dljob()
    controller.watch_crd()