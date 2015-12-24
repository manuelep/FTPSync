#!/usr/bin/env python
# -*- coding: utf-8 -*-
from gluon import *

class Libs(object):
    """ Conditional javascript/css file loading
    usage:
    Libs.append(URL('static', 'submodules/.../mylib.css/js'), 'mycontroler', 'mycontroler2.myfunction')
        The library will be loaded for all function in controller "mycontroller" and only for function "myfunction" under the controller "mycontroller2"
    """

    @staticmethod
    def append(url, *args):
        """
        args: list of strings representing controllers (and eventually
              their function where the library linked by url has to be loaded).
              Eg: Libs.append(URL(...), 'c1', 'c2.f')
        """
        c = current.request.controller
        f = current.request.function
        cf = '.'.join((c, f, ))
        if args:
            if c in args or cf in args:
                current.response.files.append(url)
        else:
            current.response.files.append(url)
