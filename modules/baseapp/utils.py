#!/usr/bin/env python
# -*- coding: utf-8 -*-

from gluon import *
from storage import Storage
import collections
import datetime

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


def prettydate(d, T=lambda x: x, utc=False, use_suffix=True, now=None):
    """ Courtesy of: https://github.com/web2py/web2py/blob/R-2.12.3/gluon/tools.py#L5531 """
    if now is None:
        now = datetime.datetime.utcnow() if utc else datetime.datetime.now()
    if isinstance(d, datetime.datetime) and isinstance(now, datetime.datetime):
        dt = now - d
    elif isinstance(d, datetime.date) and isinstance(now, datetime.date):
        dt = now - d
    elif isinstance(d, datetime.datetime) and isinstance(now, datetime.date):
        dt = d.date() - now
    elif isinstance(d, datetime.date) and isinstance(now, datetime.datetime):
        dt = d - now.date()
    else:
        return '[invalid date]'
    if dt.days < 0:
        suffix = ' from now' if use_suffix else ''
        dt = -dt
    else:
        suffix = ' ago' if use_suffix else ''
    if dt.days >= 2 * 365:
        return T('%d years' + suffix) % int(dt.days / 365)
    elif dt.days >= 365:
        return T('1 year' + suffix)
    elif dt.days >= 60:
        return T('%d months' + suffix) % int(dt.days / 30)
    elif dt.days > 21:
        return T('1 month' + suffix)
    elif dt.days >= 14:
        return T('%d weeks' + suffix) % int(dt.days / 7)
    elif dt.days >= 7:
        return T('1 week' + suffix)
    elif dt.days > 1:
        return T('%d days' + suffix) % dt.days
    elif dt.days == 1:
        return T('1 day' + suffix)
    elif dt.seconds >= 2 * 60 * 60:
        return T('%d hours' + suffix) % int(dt.seconds / 3600)
    elif dt.seconds >= 60 * 60:
        return T('1 hour' + suffix)
    elif dt.seconds >= 2 * 60:
        return T('%d minutes' + suffix) % int(dt.seconds / 60)
    elif dt.seconds >= 60:
        return T('1 minute' + suffix)
    elif dt.seconds > 1:
        return T('%d seconds' + suffix) % dt.seconds
    elif dt.seconds == 1:
        return T('1 second' + suffix)
    else:
        return T('now')
