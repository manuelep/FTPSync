#!/usr/bin/env python
# -*- coding: utf-8 -*-
from gluon import *
import datetime
from serializers import json
from gluon.utils import web2py_uuid
from jsmin import jsmin
from numpy import mean

def ICON(iname, dialect='fa', args=None, vars=None):
    """"""
    args = [] if args is None else args
    vars = {} if vars is None else vars
    if dialect=="fa":
        # <i class="fa fa-tachometer"></i>
        icon = I(*args, **vars)
        bclass = "fa fa-"
    else:
        icon = SPAN(*args, **vars)
        bclass = "glyphicon glyphicon-"
    icon.add_class(bclass+iname)
    return icon

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

class Morris(object):

    @staticmethod
    def _options(element, data, xkey, ykeys, labels, hideHover='auto', resize=True, **kw):

        KW = dict([(k,v,) for k,v in kw.iteritems() if not k.startswith('_')])

        return dict(
            element = element,
            data = data,
            xkey = xkey,
            ykeys = ykeys,
            labels = labels,
            hideHover = hideHover,
            resize = resize,
            **KW
        )

    @classmethod
    def _graph(cls, element, *args, **vars):

        attributes = dict([(k,v,) for k,v in vars.iteritems() if k.startswith('_')])
        options = cls.options(element, *args, **vars)
        return DIV(DIV(
            DIV(_id=element, **attributes),
            SCRIPT("Morris.%s(%s);" % (cls.METHOD, json(options),), _type="text/javascript"),
        _class="col-md-12"), _class="row")

class MorrisLine(Morris):

    METHOD = "Line"

    @classmethod
    def options(cls, *args, **vars):
        defaults = dict(pointSize=2)
        return cls._options(*args, **dict(defaults, **vars))

    @classmethod
    def graph(cls, element, *args, **vars):
        return cls._graph(element, *args, **vars)

class MorrisBar(Morris):

    METHOD = "Bar"

    @classmethod
    def options(cls, *args, **vars):
        defaults = dict()
        return cls._options(*args, **dict(defaults, **vars))

    @classmethod
    def graph(cls, element, *args, **vars):
        return cls._graph(element, *args, **vars)


class GraphJs(object):

    @staticmethod
    def getId(id):
        if id is None:
            return web2py_uuid()[:4]
        else:
            return id

    @classmethod
    def canvas(cls, id, **kw):
        return TAG.canvas(_id=id, _class="col-md-12", **kw)

    @classmethod
    def js(cls, method, data, id, **opts):
        _data = json(data)
        _opts = json(opts)
        return jsmin("""var ctx = document.getElementById("%(id)s").getContext("2d");
        new Chart(ctx).%(method)s(%(_data)s, %(_opts)s);
        """ % locals())

    @classmethod
    def components(cls, method, data, id=None, options=None, **kw):
        id = cls.getId(id)
        return (
            cls.canvas(id, **kw),
            cls.js(method, data, id, **(options or {}))
        )

    @classmethod
    def draw(cls, method, data, id=None, options=None, **kw):
        id = cls.getId(id)
        canvas, js = cls.components(method, data, id, options, **kw)
        return DIV(
            canvas,
            SCRIPT(js, _type="text/javascript")
        )

    @classmethod
    def singleBarChart(cls, rows, xkey, ykey, id=None, label="My First dataset",
        obar=None, ochar=None, **kw):
        
        obar_defaults = {
            "fillColor": "rgba(151,187,205,0.5)",
            "strokeColor": "rgba(151,187,205,0.8)",
            "highlightFill": "rgba(151,187,205,0.75)",
            "highlightStroke": "rgba(151,187,205,1)",
        }
        ochar_defaults = {"animation": False}
        obar = dict(obar_defaults, **(obar or {}))
        ochar = dict(ochar_defaults, **(ochar or {}))

        labels, _data = [], []
        for row in rows:
            labels.append(row[xkey])
            _data.append('%.2f' % row[ykey])
        data = {
            "labels": labels,
            "datasets": [
                dict(
                    label = label,
                    data = _data,
                    **obar
                )
            ]
        };
        return cls.draw("Bar", data, id, ochar, **kw)

    @classmethod
    def overlayChart(cls, rows, xkey, ykey, id=None,
        label="My First dataset", mean_label="Averege value",
        obar=None, oline=None, ochar=None, **kw):
        """
        overlayData = {
            labels: ["January", "February", "March", "April", "May", "Jun", "July"],
            datasets: [{
                label: "My First dataset",
                type: "bar",
                yAxesGroup: "1",
                fillColor: "rgba(151,137,200,0.5)",
                strokeColor: "rgba(151,137,200,0.8)",
                highlightFill: "rgba(151,137,200,0.75)",
                highlightStroke: "rgba(151,137,200,1)",
                data: [28, 48, 40, 19, 86, 27, 90]
            }, {
                label: "My Second dataset",
                type: "line",
                yAxesGroup: "2",
                fillColor: "rgba(151,187,205,0.5)",
                strokeColor: "rgba(151,187,205,0.8)",
                highlightFill: "rgba(151,187,205,0.75)",
                highlightStroke: "rgba(151,187,205,1)",
                data: [8, 38, 30, 29, 46, 67, 80]
            }],
            yAxes: [{
                name: "1",
                scalePositionLeft: false,
                scaleFontColor: "rgba(151,137,200,0.8)"
            }, {
                name: "2",
                scalePositionLeft: true,
                scaleFontColor: "rgba(151,187,205,0.8)"
            }]
        };
        """

        obar_defaults = {
            "fillColor": "rgba(151,187,205,0.5)",
            "strokeColor": "rgba(151,187,205,0.8)",
            "highlightFill": "rgba(151,187,205,0.75)",
            "highlightStroke": "rgba(151,187,205,1)",
        }
        ochar_defaults = {"animation": False}
        oline_defaults = {
            "fillColor": "rgba(250,88,88,0.05)",
            "pointColor": "rgba(0,0,0,0.0)",
            "pointStrokeColor": "rgba(0,0,0,0.0)",
            "strokeColor": "rgba(250,88,88,0.7)",
            
        }

        obar = dict(obar_defaults, **(obar or {}))
        oline = dict(oline_defaults, **(oline or {}))
        ochar = dict(ochar_defaults, **(ochar or {}))

        labels, _data = [], []
        for row in rows:
            labels.append(row[xkey])
            _data.append('%.2f' % row[ykey])

        _mean = mean([row[ykey] for row in rows])
        _means = ['%.2f' % _mean for i in _data]

        data = {
            "labels": labels,
            "datasets": [
                dict(
                    label = label,
                    type = "bar",
                    data = _data,
                    **obar
                ),
                dict(
                    label = mean_label,
                    type = "line",
                    data = _means,
                    **oline
                ),
            ]
        }

        return cls.draw("Overlay", data, id, ochar, **kw)