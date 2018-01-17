import logging
import os
import sys

import yaml

config = yaml.load(open(os.path.join(sys.path[0], 'config.yaml')))

logger = logging.getLogger()
logger.setLevel(logging.INFO)
handler = logging.StreamHandler(sys.stdout)
handler.setFormatter(logging.Formatter('[%(asctime)s] %(filename)s[%(lineno)d]# %(levelname)-8s %(message)s'))
handler.setLevel(logging.INFO)
logger.addHandler(handler)
