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
import xlrd, csv, paramiko

def sizeof_fmt(num, suffix='B'):
    for unit in ['','K','M','G','T','P','E','Z']:
        if abs(num) < 1024.0:
            return "%3.1f %s%s" % (num, unit, suffix)
        num /= 1024.0
    return "%.1f %s%s" % (num, 'Y', suffix)

def _get_dest_parts2(archname=None):

    out = {"path": "", "tmp_path": "/tmp"}

    dest = current.appconf.get("dest", {}).get("dest")
    if not dest is None:
        out.update(current.appconf[dest])
        if not "protocol" in out:
            out["protocol"] = dest.split("_")[0]

    out.update({k: v for k,v in current.appconf.get("dest", {}).iteritems() if k!="dest"})
    prefix = "dest_"
    out.update({k[len(prefix):]: v for k,v in current.appconf.get(archname, {}).iteritems() if k.startswith(prefix)})

    return out

class Elaborate(object):

    @staticmethod
    def _get_new_path(an, fp, i=None):
        """
        an @string  : Archive name
        fp @string  : File path
        i  @integer : Index
        """
        pt, fn = os.path.split(fp)
        _raw_uuids = current.appconf[an].get("csvuuids")
        uuids = _raw_uuids and _raw_uuids.split(',')
        dest_nfo = _get_dest_parts2(an)

        rpl = ".csv" if i is None else "_tab%s.csv" % i
        nfn = (dest_nfo.get("name") or fn).replace(".xls", rpl)

        if uuids:
            try:
                mypath = os.path.join(pt, uuids[i or 0])
            except IndexError:
                return None
            else:
                if not os.path.exists(mypath):
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
    def unzip(cls, tab, row):
        """ Unzip only first file in temporary local path """
        dest_nfo = _get_dest_parts2(row.archname)
        dest_path = dest_nfo.get("path")[1:] if dest_nfo.get("path").startswith("/") else dest_nfo.get("path")
        tmp_path = dest_nfo.get("path")
        if not tmp_path is None:
            (filename, stream) = tab.archive.retrieve(row.archive)
            with zipfile.ZipFile(stream, "r") as adv:
                advfilenames = adv.namelist()
                assert len(advfilenames)==1, "Unsupported!"
                advfilename = advfilenames[0]

                mypath = os.path.join(tmp_path, dest_path)
                if not os.path.exists(mypath):
                    os.makedirs(mypath)
                
                ext_path = os.path.join(mypath, advfilename)
                adv.extract(advfilename, ext_path)
            return ext_path

    @classmethod
    def copy2(cls, tab, row, rename=True):
        """ Copy result file in temporary local path """
        dest_nfo = _get_dest_parts2(row.archname)
        dest_path = dest_nfo.get("path")[1:] if dest_nfo.get("path").startswith("/") else dest_nfo.get("path")
        tmp_path = dest_nfo.get("tmp_path")
        if not dest_nfo.get("path") is None:
            (filename, stream) = tab.archive.retrieve(row.archive)
            if rename:
                dest_name = dest_nfo.get("name") or filename
            else:
                dest_name = filename
            dest_path = os.path.join(tmp_path, dest_path)
            if not os.path.exists(dest_path):
                os.makedirs(dest_path)
            dest_file_path = os.path.join(dest_path, dest_name)
            shutil.copyfileobj(stream, open(dest_file_path, 'wb'))

            current.logger.debug("File %s successfully copied to: %s" % (dest_name, dest_path,))
            return dest_file_path

    @staticmethod
    def _get_refills():
        """ Returns a dictionary with categories as keys and refills as values """
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
    def _export_sheet_to_csv(cls, sheet, newcsvpath, priceCol=None, catCol="Nome Categoria Catalogo"):
        """
        sheet      @ADVSheetObject;
        newcsvpath @string : Path where to save the sheet in a single csv file; 
        priceCol   @string : Name of the column to which apply th refill;
        catCol     @string : Name of the column of categories used to get the right refill.
        WARNING: Only if priceCol is given it will try to apply refill.
        """

        if not priceCol is None:
            default_refill = 30
            refills = cls._get_refills()
            _get_refill = lambda cat: float(refills.get(cat, 0)) or default_refill

        def _get_full_price(row):
            refill = _get_refill(row[catCol])

            def _get_price():
                price = row[priceCol]
                if price in (None, "n.d.",):
                    return None
                if isinstance(price, basestring):
                    return float(price.replace(",", "."))
                else:
                    return price

            raw_base_price = _get_price()

            if raw_base_price is None:
                return None
            else:
                full_price = (1+refill/100.)*raw_base_price
                rounded_price = "%.2f" % full_price
                return rounded_price.replace(".", ",")

        new_col_head = "Prezzo Newirbel"
        allvalues = cls._read_not_empty_rows_from_sheet(sheet)
        fieldnames=allvalues.next()
        if not priceCol is None:
            fieldnames += [new_col_head]

        with open(newcsvpath, "w") as ADVcsv:
            csvwriter = csv.DictWriter(ADVcsv, fieldnames=fieldnames)
            csvwriter.writeheader()
            for values in allvalues:
                csv_row_content = dict(zip(fieldnames, values))
                if not priceCol is None:
                    full_price = _get_full_price(csv_row_content)
                    if not full_price is None:
                        csv_row_content[new_col_head] = full_price
                csvwriter.writerow(csv_row_content)

    @classmethod
    def extract_tabs(cls, archname, filepath, priceCol="prezzo", priceTab=0, removeOriginal=True):
        """ Split tabs of a single xls file into multiple csv """

        with xlrd.open_workbook(filepath) as xlsSRC:
            for sindex in xrange(xlsSRC.nsheets):
                sheet = xlsSRC.sheet_by_index(sindex)
                newcsvpath = cls._get_new_path(archname, filepath, sindex)
                if not newcsvpath is None:
                    if sindex == priceTab:
                        cls._export_sheet_to_csv(sheet, newcsvpath, priceCol=priceCol)
                    else:
                        cls._export_sheet_to_csv(sheet, newcsvpath, priceCol=None)

        if removeOriginal:
            os.remove(filepath)

    @classmethod
    def run(cls, tab, row, removeTmp=False):
        # Prezzi
        if row.archname == "arch_pri":
            fp = cls.unzip(tab, row)
            cls.extract_tabs(row.archname, fp, priceCol="Prezzo Base Rivenditore", removeOriginal=removeTmp)
        # Catalogo
        elif row.archname == "arch_cat":
            fp = cls.copy2(tab, row, rename=False)
            cls.extract_tabs(row.archname, fp, removeOriginal=removeTmp)
        # Altro
        else:
            cls.copy2(tab, row, rename=True)

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

    @staticmethod
    def _get_dest_parts():

        out = {}

        if "dest" in current.appconf:
            dest = current.appconf["dest"]["dest"]
            out.update({k: v for k,v in current.appconf["dest"].iteritems() if k!="dest"})
        else:
            dest = None

        if not dest is None:
            out.update(current.appconf[dest])
            out["protocol"] = dest.split("_")[0]

        return out or None

    def rsync(self):
        dest_nfo = self._get_dest_parts()

        if dest_nfo.get("protocol") == "ssh":
            # Copy to remote destination using SSH/SFTP protocol with Paramiko

            current.logger.debug("File transfer via SFTP is starting!")
            client = paramiko.SSHClient()
            client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            conn = client.connect(dest_nfo["url"], 22,
                username = dest_nfo["user"],
                password = dest_nfo["passwd"],
                timeout = 180
            )
            start = datetime.datetime.now()
            sftp = client.open_sftp()
    
            for __dirpath, __dirnames, filenames in os.walk(Elaborate.path):
                for filename in filenames:
                    source_file_path = os.path.join(Elaborate.path, filename)
                    dest_file_path = os.path.join(dest_nfo["path"], filename)
                    # Use paramiko: http://stackoverflow.com/a/11519239/1039510
                    err = False
                    try:
                        sftp.put(source_file_path, dest_file_path)
                    except Exception as error:
                        err = True
                    finally:
                        if conn:
                            conn.close()
                            logger.debug("File transfer via SFTP started: %s" % prettydate(start))
                        if err:
                            raise error
                shutil.rmtree(Elaborate.path)
                os.makedirs(Elaborate.path)
        elif dest_nfo.get("protocol") is None:
            # Copy to local destination
            start = datetime.datetime.now()
            for __dirpath, __dirnames, filenames in os.walk(Elaborate.path):
                for filename in filenames:
                    source_file_path = os.path.join(Elaborate.path, filename)
                    dest_file_path = os.path.join(dest_nfo["path"], filename)
                    shutil.move(source_file_path, dest_file_path)
            logger.debug("Local file transfer started: %s" % prettydate(start))
        else:
            raise NotImplementedError

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

        def _get_last_path(*path):
            _dir = path[-1]
            if _dir.endswith("YYYY"):
                dpath = os.path.join(*path[:-1])
                _dirs = tuple(sorted(filter(lambda n: n.startswith(_dir[:-4]), self.ftp.nlst(*dpath)), key=lambda m: m[-4:]))
                return os.path.join(*(path[:-1]+_dirs[-1:]))
            else:
                return os.path.join(*path)

        now = datetime.datetime.now()

        rpath = remote_path
        remote_path = _get_last_path(*os.path.split(rpath))
        files = self.ftp.nlst(remote_path)
        nfiles = len(files)
        for n,filename in enumerate(files):
            filepath = os.path.join(remote_path, filename)

            start = datetime.datetime.now()
            if (name_starts is None or filename.lower().startswith(name_starts.lower())) and (extension is None or filename.endswith(extension)):

                if current.development:
                    self.ftp.sendcmd("TYPE i")
                    try:
                        filesize = self.ftp.size(filepath)
                    except:
                        import pdb;pdb.set_trace()
                    self.ftp.sendcmd("TYPE A")
                    current.logger.debug("File: %s (size: %s)" % (filename, sizeof_fmt(filesize),))

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
                    Elaborate.run(self.table, row, removeTmp=(not current.development))
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
                            Elaborate.run(self.table, row, removeTmp=(not current.development))
                        else:
                            current.logger.info("It's time to update file but no new version found")
                    else:
                        current.logger.info("File still updated.")
            else:
                current.logger.debug("%s, %s" % (name_starts, extension,))
            current.logger.info("Fetched %s files on %s" % (n+1, nfiles,))
            current.logger.info("=== Fetching file operation terminated (Started: %s) ===\n" % prettydate(start))
