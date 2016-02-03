#!/usr/bin/env python
# -*- coding: utf-8 -*-
from gluon import *
from gluon import current
from gluon.storage import List
from ftplib import FTP
from io import BytesIO
from baseapp.utils import prettydate
import datetime
import hashlib
import os
import shutil
import zipfile
import xlrd, csv

class Elaborate(object):

    @staticmethod
    def _get_dest_parts(archname):
        return (
            current.appconf[archname].get("dest_path") or current.appconf.get("misc", {}).get("dest_path"),
            current.appconf[archname].get("dest_name"),
        )

    @staticmethod
    def _get_new_path(an, fp, i=None):
        pt, fn = os.path.split(fp)
        _raw_uuids = current.appconf[an].get("csvuuids")
        uuids = _raw_uuids and _raw_uuids.split(',')
        if i is None:
            nfn = fn.replace(".xls", ".csv")
        else:
            nfn = fn.replace(".xls", "_tab%s.csv" % i)

        if uuids:
            mypath = os.path.join(pt, uuids[i or 0])
            if current.development and not os.path.exists(mypath):
                os.makedirs(mypath)
            return os.path.join(mypath, nfn)
        else:
            return os.path.join(pt, nfn)

    @staticmethod
    def _read_not_empty_rows_from_sheet(sheet):
        def _uenc(cell):
            """ Encode unicode value to system encoding """
            value = cell.value
            if isinstance(value, basestring):
                return value.encode("utf8")
            else:
                return value
        for ridx in xrange(sheet.nrows):
            values = [_uenc(c) for c in sheet.row(ridx)]
            if any(values):
                yield values
            else:
                continue

    @classmethod
    def copy(cls, tab, row):
        """ Copy file to their destination """
#         assert row.archname in ("arch_2", "arch_3", "arch_4", "arch_5",), "Wrong archive!"
        dest_path, dest_name = cls._get_dest_parts(row.archname)
        if not dest_path is None:
            (filename, stream) = tab.archive.retrieve(row.archive)
            if dest_name is None:
                dest_name = filename

            dest_file_path = os.path.join(dest_path, dest_name)
            shutil.copyfileobj(stream, open(dest_file_path, 'wb'))
            current.logger.debug("File %s successfully copied to: %s" % (dest_name, dest_path,))
            return dest_file_path

    @staticmethod
    def _get_refills(): 
        cpath = os.path.join(current.appconf.conf_price.path, "%(filename)s.%(extension)s" % current.appconf.conf_price)
        def _iter():
            with open(cpath, 'rb') as csvfile:
                spamreader = csv.reader(csvfile, delimiter=',')
                for row in spamreader:
                    _row = List(*row)
                    yield _row(0), _row(1)

        if os.path.isfile(cpath):
            refills = dict([(k,v) for k,v in _iter()])
        else:
            refills = {}
        return refills
                    
 
    @classmethod
    def _apply_refills(cls, filepath, archname):
        default_refill = 15
        refills = cls._get_refills()
        _get_refill = lambda cat: float(refills.get(cat, 0)) or default_refill

        def _get_full_price(row):
            refill = _get_refill(row["Categoria"])

            raw_base_price = row["Prezzo Base Rivenditore"].strip()

            if not raw_base_price:
                return None
            else:
                base_price = float(raw_base_price.replace(",", "."))
                full_price = (1+refill/100.)*base_price
                rounded_price = "%.2f" % full_price
                return rounded_price.replace(".", ",")

        newfilepath = cls._get_new_path(archname, filepath)

        with xlrd.open_workbook(filepath) as ADVxls, open(newfilepath, "w") as ADVcsv:
            ADVsheet = ADVxls.sheet_by_index(0)
            new_col_head = "Prezzo Newirbel"

            allvalues = cls._read_not_empty_rows_from_sheet(ADVsheet)
            fieldnames=allvalues.next()+[new_col_head]
            csvwriter = csv.DictWriter(ADVcsv, fieldnames=fieldnames)
            csvwriter.writeheader()
            for values in allvalues:
                csv_row_content = dict(zip(fieldnames, values))
                full_price = _get_full_price(csv_row_content)
                if not full_price is None:
                    csv_row_content[new_col_head] = full_price
                csvwriter.writerow(csv_row_content)

    @classmethod
    def ADVFileProcess(cls, tab, row):
        """
        Unzip ADVFile archive content to their destination and convert xls
        format to csv applying refill to base price.
        """
#         assert row.archname=="arch_1", "Wrong archive!"
        dest_path, dest_name = cls._get_dest_parts(row.archname)
        if not dest_path is None:
            (filename, stream) = tab.archive.retrieve(row.archive)
            with zipfile.ZipFile(stream, "r") as adv:
                advfilename = adv.namelist()[0]
                adv.extractall(dest_path)
            if not dest_name is None:
                dest_file_path = os.path.join(dest_path, dest_name)
                os.rename(os.path.join(dest_path, advfilename), dest_file_path)
            else:
                dest_file_path = os.path.join(dest_path, advfilename)
            cls._apply_refills(dest_file_path, row.archname)

    @classmethod
    def extract_tabs(cls, tab, row):
        """ Split tabs of a single xls file into multiple csv """

        filepath = cls.copy(tab, row)
        if not filepath is None:
            
            with xlrd.open_workbook(filepath) as xlsSRC:
                for sindex in xrange(xlsSRC.nsheets):
                    sheet = xlsSRC.sheet_by_index(sindex)
                    newfilename = cls._get_new_path(row.archname, filepath, sindex)
                    allvalues = cls._read_not_empty_rows_from_sheet(sheet)

                    with open(newfilename, "w") as csvDEST:
                        fieldnames = allvalues.next()
                        csvwriter = csv.DictWriter(csvDEST, fieldnames=fieldnames)
                        csvwriter.writeheader()
                        for values in allvalues:
                            csv_row_content = dict(zip(fieldnames, values))
                            csvwriter.writerow(csv_row_content)

    @classmethod
    def run(cls, tab, row):
        # Prezzi
        if row.archname == "arch_1":
            cls.ADVFileProcess(tab, row)
        # Catalogo
        elif row.archname == "arch_3":
            cls.extract_tabs(tab, row)
        else:
            cls.copy(tab, row)
    

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

    def fetch(self, archname, remote_path, period, name_starts=None, extension=None, **kw):
        now = datetime.datetime.now()
        for filepath in self.ftp.nlst(remote_path):
            path, filename = os.path.split(filepath)
            current.logger.info("File: %s" % filename)

            if (name_starts is None or filename.startswith(name_starts)) and (extension is None or filename.endswith(extension)):
                is_in_db = self.db(self.table.filename==filename).count()
                last_fs_update = datetime.datetime.strptime(self.ftp.sendcmd('MDTM ' + filepath)[4:], "%Y%m%d%H%M%S")

                if is_in_db==0:
                    current.logger.debug("New file detected and downloaded.")
                    newfilename, filehash = self.retrieve(filepath)
                    id = self.table.insert(
                        filename = filename,
                        archname = archname,
                        archive = newfilename,
                        last_update = last_fs_update,
                        checksum = filehash
                    )
                    row = self.table[id]
                    self.db.commit()
                    Elaborate.run(self.table, row)
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
                            Elaborate.run(self.table, row)
                        else:
                            current.logger.info("It's time to update file but no new version found")
                    else:
                        current.logger.info("File still updated.")
            current.logger.info("=== Fetching file operation terminated ===")
