#!/usr/bin/env python
# -*- coding: utf-8 -*-
from gluon import *
from storage import Storage

import collections

class nested(object):

    @classmethod
    def load(cls, c, d=None, nest_storages=True, **casts):
        """
        d @dict or @Storage :
        c @object (AppConig) : AppConfig object
        hook @function(value, *path) : Cast function
        """

        if d is None:
            d = Storage() if nest_storages else dict()

        for k, v in c.iteritems():
            if isinstance(v, collections.Mapping):
                if nest_storages:
                    r = cls.load(v, d.get(k, Storage()), **casts)
                else:
                    r = cls.load(v, d.get(k, {}), **casts)
                d[k] = r
            else:
                cast = casts.get(k, lambda v: v)
                d[k] = cast(c.get(k))
        return d

    @classmethod
    def update(cls, d, u):
        """ Coutesy of: http://stackoverflow.com/questions/3232943/update-value-of-a-nested-dictionary-of-varying-depth """
        for k, v in u.iteritems():
            if isinstance(v, collections.Mapping):
                r = cls.update(d.get(k, {}), v)
                d[k] = r
            else:
                d[k] = u[k]
        return d

    @classmethod
    def compare(cls, d, u):
        for k, v in u.iteritems():
            if not k in d:
                return False
            elif isinstance(v, collections.Mapping):
                if isinstance(d[k], collections.Mapping):
                    rr = cls.compare(d[k], v)
                    if rr ==True:
                        continue
                    else:
                        return False
                else:
                    return False
            else:
                if not d[k] == v:
                    return False
                else:
                    continue
        return True
