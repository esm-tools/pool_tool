from .analyse import *


def loadconf():
    import os
    import yaml
    import shutil

    default_config_file = os.path.join(os.path.dirname(__file__), "ptool_config.yaml")
    user_config = os.path.expanduser("~/.ptool.yaml")
    if not os.path.exists(user_config):
        shutil.copy(default_config_file, user_config)
    with open(user_config) as fid:
        conf = yaml.safe_load(fid)
    return conf


#conf = loadconf()
del loadconf
