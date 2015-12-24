# -*- coding: utf-8 -*-

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

appconf = nested.load(myconf,
    migrate = bool,
    pool_size = int,
    period = int
)

current.appconf = appconf


#                                                                 ### LOGGER ###

logger = get_configured_logger(request.application)
current.logger = logger
