#!/usr/bin/env python
# -*- coding: utf-8 -*-
from gluon import *

import time, oath, hashlib, base64, qrcode
# from gluon.utils import web2py_uuid
from sqlhtml import StringWidget

from gluon import current

try:
    from cStringIO import StringIO
except ImportError:
    from StringIO import StringIO

# UTILS

def fromhex(s):
    return s.decode('hex')

def tohex(bin):
    return bin.encode('hex')


# CORE

def get_hotp_token(secret, intervals_no, exp=6):
    if exp == 8:
        return oath.hotp(secret, intervals_no, format='dec8', hash=hashlib.sha256)
    elif exp == 6:
        return oath.hotp(secret, intervals_no, format='dec6', hash=hashlib.sha256)

def get_totp_token(secret, exp=8):
    return get_hotp_token(secret, intervals_no=int(time.time())//30, exp=exp)


# def get_server_secret():
#     return tohex(web2py_uuid())
# 
# 
# def token_test(code, secret):
#     return code == get_totp_token(secret, 8)

class SecretCodeWidget(StringWidget):

    @classmethod
    def widget(cls, field, value, **attributes):
        """ Courtesy of: http://jsfiddle.net/briguy37/2mvfd/
        HTML: 
        <div class="input-group">
          <input type="text" class="form-control" aria-label="...">
          <div class="input-group-btn">
            <!-- Button and dropdown menu -->
          </div>
        </div>
        <div class="input-append">
          <input  id="uuid" class="span2" type="text">
          <button id="generateUUID" class="btn" type="button">New UUID!</button>
        </div>
        JS:
        function generateUUID() {
            var d = new Date().getTime();
            var uuid = 'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'.replace(/[xy]/g, function(c) {
                var r = (d + Math.random()*16)%16 | 0;
                d = Math.floor(d/16);
                return (c=='x' ? r : (r&0x3|0x8)).toString(16);
            });
            return uuid;
        };
        document.getElementById('generateUUID').onclick = function() {
            document.getElementById('%(_id)s').value = generateUUID();
        };
        
        document.getElementById('%(_id)s').innerHTML = generateUUID();
        """

        script = """function generateUUID() {
    var d = new Date().getTime();
    var uuid = 'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'.replace(/[xy]/g, function(c) {
        var r = (d + Math.random()*16)%%16 | 0;
        d = Math.floor(d/16);
        return (c=='x' ? r : (r&0x3|0x8)).toString(16);
    });
    return uuid;
};

document.getElementById('generateUUID').onclick = function() {
    document.getElementById('%(_id)s').value = generateUUID();
};

document.getElementById('%(_id)s').innerHTML = generateUUID();"""

        default = dict(
            _type='text',
            value=(value is not None and str(value)) or '',
        )
        attr = cls._attributes(field, default, **attributes)
        input = INPUT(**attr)
        input.add_class("form-control")

        return DIV(
            input,
            DIV(TAG.button(I(_class="fa fa-qrcode"), " ", current.T("New code"), _id="generateUUID", _class="btn btn-default", _type="button"),
                _class="input-group-btn"),
            SCRIPT(script % attr, _type="text/javascript"),
            _class="input-group"
        )

def get_qr_b(raw, email, name="foo", type="totp", period=30, algorithm="SHA256", digits=8):

    secret = base64.b32encode(raw)
    url = "otpauth://totp/%(name)s:%(email)s?type=%(type)s&period=%(period)s&algorithm=%(algorithm)s&digits=%(digits)s&secret=%(secret)s" % locals()
    qr = qrcode.QRCode()
    qr.add_data(url)
    qr.make(fit=True)
    img = qr.make_image()
    img_buf = StringIO()
    img.save(img_buf)
    img_buf.seek(0)

    return img_buf.read()

class IS_MY_OTP_TOKEN(object):

    def __init__(self, email, password, auth, error_message="Fail!"):
        self.email = email
        self.password = password
        self.auth = auth
        self.error_message = error_message

    def __call__(self, value):

        user = self.auth.login_bare(self.email, self.password)
        
        raw = user.otp_secret
        if not raw:
            return (value, None)
        else:
            check_value = get_totp_token(tohex(raw))
            if value == check_value:
                return (value, None)
            else:
                current.logger.warning("user %s tried login with a wrong otp token: %s" % (self.email, value, ))
                return (value, self.error_message)
