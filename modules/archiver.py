#!/usr/bin/env python
# -*- coding: utf-8 -*-
from gluon import *
from ftplib import FTP
from io import BytesIO
from app.utils import prettydate
import datetime
import hashlib
import os

class Digger(object):
    """"""

    def __init__(self, url, user, passwd, table, checksum_required=True):
        self.url = url
        self.user = user
        self.passwd = passwd
        self.table = table
        self.db = table._db
        self.checksum_required = checksum_required

    def __enter__(self):
        self.ftp = FTP(self.url)
        self.ftp.login(user=self.user, passwd=self.passwd)
        return self

    def __exit__(self, exception_type, exception_value, traceback):
        self.ftp.quit()

    def _truncate(self):
        self.table.truncate('RESTART IDENTITY CASCADE')

    def retrieve(self, filename):
        """
        Download single file from FTP repository
        filename @string : name of file to download
        
        returns (newfilename, hex-md5-checksum,)
        """
        stream = BytesIO()
        start = datetime.datetime.now()
        self.ftp.retrbinary('RETR ' + filename, stream.write, 1024)
        current.logger.debug("File '%s' download started: %s" % (filename, prettydate(start),))
        stream.seek(0)
        if self.checksum_required:
            filehash = hashlib.new('sha224')
            filehash.update(stream.read())
            checksum = filehash.hexdigest()
            stream.seek(0)
        else:
            checksum = None
        newfilename = self.table.archive.store(stream, filename)
        current.logger.debug("Original file name: %s" % filename)
        current.logger.debug("New file name: %s" % newfilename)
        current.logger.debug("checksum: %s" % checksum)
        stream.close()
        return newfilename, checksum

    def fetch(self, archname, path, period, name_starts=None, extension=None, **kw):
        now = datetime.datetime.now()
        for filepath in self.ftp.nlst(path):
            path, filename = os.path.split(filepath)
            current.logger.info("File: %s" % filename)

            if (name_starts is None or filename.startswith(name_starts)) and (extension is None or filename.endswith(extension)):
                is_in_db = self.db(self.table.filename==filename).count()
                last_fs_update = datetime.datetime.strptime(self.ftp.sendcmd('MDTM ' + filepath)[4:], "%Y%m%d%H%M%S")

                if is_in_db==0:
                    current.logger.debug("New file detected and downloaded.")
                    newfilename, filehash = self.retrieve(filepath)
                    self.table.insert(
                        filename = filename,
                        archname = archname,
                        archive = newfilename,
                        last_update = last_fs_update,
                        checksum = filehash
                    )
                    self.db.commit()
                else:
                    row = self.db(self.db.archive.filename==filename).select(limitby=(0,1)).first()
                    if (now-row.last_update).total_seconds()>=period:
                        if row.last_update < last_fs_update:
                            current.logger.debug("Detected file that needs update.")
                            newfilename, filehash = self.retrieve(filepath)
                            row.update_record(
                                archive = newfilename,
                                last_update = last_fs_update,
                                checksum = filehash,
    #                             is_active = True
                            )
                            self.db.commit()
                        else:
                            current.logger.info("It's time to update file but no new version found")
                    else:
                        current.logger.info("File still updated.")
            current.logger.info("=== Fetching file operation terminated ===")
