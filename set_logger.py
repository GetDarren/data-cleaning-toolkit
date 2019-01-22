import logging

logging.basicConfig(filename="DSL_ingestion", 
            level=logging.DEBUG,
            format='%(asctime)s.%(msecs)03d %(levelname)s %(module)s - %(funcName)s: %(message)s', 
            datefmt='%Y-%m-%d %H:%M:%S')
logger = logging.getLogger(__name__)
logger.addHandler(logging.StreamHandler())
logger.info('=> START SCRIPT')