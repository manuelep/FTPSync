# -*- coding: utf-8 -*-

import os
from gluon import current
from storage import Storage
from baseapp.utils import nested
from baseapp.log import get_configured_logger
from curses.ascii import isdigit


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

def lpath(p):
    """ Local path """
    if p.startswith("/"):
        mypath = p if not current.development else os.path.expanduser('~'+p)
    else:
        mypath = os.path.join(os.getcwd(), request.folder, p)
    if not os.path.exists(mypath):
        os.makedirs(mypath)
    return mypath

def boolean(v):
    v = v.strip()
    if v:
        if isdigit(v):
            return bool(int(v))
    return bool(v)

appconf = nested.load(myconf,
    migrate = boolean,
    pool_size = int,
    period = int,
    tmp_path = lpath,
    ignore = boolean
)

current.appconf = appconf


#                                                                 ### LOGGER ###

logger = get_configured_logger(request.application or "debug")
current.logger = logger
