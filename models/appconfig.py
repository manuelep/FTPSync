# -*- coding: utf-8 -*-

import os
from gluon import current
from storage import Storage
from baseapp.utils import nested
from baseapp.log import get_configured_logger


#                                                            ### CONF LOADER ###

try:
    current.development
except AttributeError:
    from gluon.contrib.appconfig import AppConfig
    myconf = AppConfig(reload=False)
    current.development = False
else:
    from baseapp.appconfig import AppConfig
    myconf = AppConfig('appconfig-dev.ini', reload=True)

#current.myconf = myconf

def path(p):
    if p.startswith("/"):
        if current.development:
            mypath = os.path.expanduser('~'+p)
        else:
            mypath = p
    else:
        mypath = os.path.join(os.getcwd(), request.folder, p)
    if not current.development and not os.path.exists(mypath):
        os.makedirs(mypath)
    return mypath

appconf = nested.load(myconf,
    migrate = bool,
    pool_size = int,
    period = int,
    dest_path = path,
    path = path
)

current.appconf = appconf


#                                                                 ### LOGGER ###

logger = get_configured_logger(request.application)
current.logger = logger
