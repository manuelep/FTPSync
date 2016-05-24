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

get_dest_parts = _get_dest_parts2

def _keep_fs_clean(filepath):
    os.remove(filepath)
    tmppath, _ = os.path.split(filepath)
    try:
        os.removedirs(tmppath)
    except OSError as err:
        current.logger.debug("Folder %s NOT removed!" % tmppath)
        current.logger.debug(str(err))
    current.logger.info("File %s removed!" % filepath)

keep_fs_clean = _keep_fs_clean

def _get_new_path(an, pt, fn, i=None):
    """
    an @string  : Archive name;
    pt @string  : Path to file;
    fn @string  : Fiel name;
    i  @integer : Tab index.
    """
    _raw_uuids = current.appconf[an].get("csvuuids")
    uuids = _raw_uuids and _raw_uuids.split(',')
    dest_nfo = _get_dest_parts2(an)

    rpl = ".csv" if i is None else "_tab%s.csv" % i
    nfn = (dest_nfo.get("name") or fn).replace(".xls", rpl)

    if uuids:
        try:
            pt = os.path.join(pt, uuids[i or 0])
        except IndexError:
            pass
        else:
            if not os.path.exists(pt):
                os.makedirs(pt)
    return os.path.join(pt, nfn)

get_new_path = _get_new_path

def _get_dest_path(archname):
    """ Build destination path from configuration and eventually create missing directories """
    dest_nfo = get_dest_parts(archname)
    dest_path = dest_nfo.get("path")[1:] if dest_nfo.get("path").startswith("/") else dest_nfo.get("path")
    tmp_path = dest_nfo["tmp_path"]
    mypath = os.path.join(tmp_path, dest_path)
    if not os.path.exists(mypath):
        os.makedirs(mypath)
    return mypath

get_dest_path = _get_dest_path

class SheetReader(object):

    def __init__(self, sheet, header_line=0):
        self.sheet = sheet
        self.start = header_line+1
        self.header = self._read_line(header_line)

    def _read(self, start=0, end=None):

        def _uenc(cell):
            """ Encode unicode value to system encoding """
            value = cell.value
            if isinstance(value, basestring):
                return value.encode("utf8")
            else:
                return value

        for ridx in xrange(start, end or self.sheet.nrows):
            values = [_uenc(c) for c in self.sheet.row(ridx)]
            if any(values):
                yield values
            else:
                continue

    def _read_line(self, n):
        return [i for i in self._read(n, n+1)][0]

    def __call__(self):
        for r in self._read(self.start):
            yield dict(zip(self.header, r))

class stock(object):

    sku = "Product SKU"

    # "Product ID","Product SKU","Product Name",Price,"In Stock","Stock Quantity"
    api = [
        {"name": sku, "cast": lambda l: l[0:6], "limits": (1,6,)},
        {"name": "Product Name", "cast": lambda l: l[6:82].strip(), "limits": (7, 82,)},
        {"name": "Price"},
        # If the value of the In Stock indicator is empty, zero, or "no", the product is considered to be out of stock;
        # all other values are taken to mean that the product is in stock.
        {"name": "In Stock", "cast": lambda *_, **__: "X"},
        {"name": "Stock Quantity", "cast": lambda l: int(l[82:87]), "limits": (83, 87,)}
    ]

    @classmethod
    def header(cls):
        return [i["name"] for i in cls.api]

    @classmethod
    def read(cls, stream):
        for line in stream.readlines():
            yield {nfo["name"]: nfo.get("cast", lambda *_: "")(line) for nfo in cls.api}
        
class Refiller(object):

    def __init__(self):
        self.refills = self._get_refills()
        self.default_refill = current.appconf.conf_price.default_refill

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

    def __call__(self, cat, v):
        """
        cat @string : Categoria
        v    @float : Valore del prezzo base rivenditore
        """
        r = float(self.refills.get(cat, 0)) or self.default_refill
        return ((100+r)/100.)*v

def load_duplicates(fp):

    puk = "Codice Spicers"
    cats = (
        "Nome Categoria Catalogo",
        "Nome Gruppo Catalogo Standard",
        "Nome Sottogruppo Catalogo standard",
    )

    out = {}
    with open(fp) as source:
        for row in csv.DictReader(source):
            if not row[puk] in out:
                out[row[puk]] = []
            rr = dict([(k, row[k]) for k in cats if row[k]])
            if all(rr.values()) and not rr in out[row[puk]]:
                out[row[puk]].append(rr)
    return out

def rsync():

    dest_nfo = _get_dest_parts2()

    if dest_nfo.get("protocol") == "ssh":
        # Copy to remote destination using SSH/SFTP protocol with Paramiko
        current.logger.info("File transfer via SFTP is starting!")
        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        conn = client.connect(dest_nfo["url"], 22,
            username = dest_nfo["user"],
            password = dest_nfo["passwd"],
            timeout = 15*60
        )
        start = datetime.datetime.now()
        sftp = client.open_sftp()

        for dirpath, __dirnames, filenames in os.walk(dest_nfo["tmp_path"]):
            if dirpath==dest_nfo["tmp_path"]:
                continue
            for filename in filenames:
                source_path = os.path.join("/", os.path.relpath(dirpath, dest_nfo["tmp_path"]))
                source_file_path = os.path.join(dirpath, filename)
                dest_file_path = os.path.join(source_path, filename)
                err = False
                try:
                    sftp.put(source_file_path, dest_file_path)
                except Exception as error:
                    err = True
                else:
                    if filename.endswith(".zip"):

                        commands = [
                            'unzip -o -d %(source_path)s %(dest_file_path)s' % locals(),
                            # Rimozione degli archivi originari (solo se unzip ha successo [1])
                            'rm -f %(dest_file_path)s' % locals(),
                            # I due passaggi che seguono sono utili SOLO per l'archivio delle immagini delle ditte
                            'mv %(source_path)s/images/* %(source_path)s/' % locals(),
                            'rmdir %(source_path)s/images' % locals(),
                        ]

                        exit_status = 0
                        for n,command in enumerate(commands):
                            # [1] Non eseguo il comando se il passo precedente non è uscito con successo
                            if command and exit_status==0 or n!=1:
                                current.logger.info("Executing: %(command)s" % locals())
                                chan = client.get_transport().open_session()
                                chan.exec_command(command)
                                # wait for result
                                exit_status = chan.recv_exit_status()
                                if exit_status==0:
                                    current.logger.info("Command exited with success!")
                                else:
                                    current.logger.error("Running command raised exception:")
                                    current.logger.info(chan.makefile_stderr().read())
                                chan.close()
                finally:
                    if err:
                        if conn:
                            conn.close()
                        raise error
                    else:
                        current.logger.info("File transfer via SFTP started: %s" % prettydate(start))
                        current.logger.info("Transfered %s to %s" % (source_file_path, dest_file_path))
        if conn:
            conn.close()

    elif dest_nfo.get("protocol") is None:
        # Copy to local destination
        start = datetime.datetime.now()
        for __dirpath, __dirnames, filenames in os.walk(dest_nfo["tmp_path"]):
            for filename in filenames:
                source_file_path = os.path.join(dirpath, filename)
                dest_path = os.path.join("/", os.path.relpath(dirpath, dest_nfo["tmp_path"]))
                dest_file_path = os.path.join(dest_path, filename)
                shutil.move(source_file_path, dest_file_path)
        current.logger.debug("Local file transfer started: %s" % prettydate(start))
    else:
        raise NotImplementedError

    if not current.development:
        shutil.rmtree(dest_nfo["tmp_path"])


# class Elaborate(object):
#     """ DEPRECATED Basic file operations """
# 
#     @staticmethod
#     def _get_new_path(an, fp, i=None):
#         """
#         an @string  : Archive name
#         fp @string  : File path
#         i  @integer : Index
#         """
#         pt, fn = os.path.split(fp)
#         _raw_uuids = current.appconf[an].get("csvuuids")
#         uuids = _raw_uuids and _raw_uuids.split(',')
#         dest_nfo = _get_dest_parts2(an)
# 
#         rpl = ".csv" if i is None else "_tab%s.csv" % i
#         nfn = (dest_nfo.get("name") or fn).replace(".xls", rpl)
# 
#         if uuids:
#             try:
#                 mypath = os.path.join(pt, uuids[i or 0])
#             except IndexError:
#                 return None
#             else:
#                 if not os.path.exists(mypath):
#                     os.makedirs(mypath)
#                 return os.path.join(mypath, nfn)
#         else:
#             return os.path.join(pt, nfn)
# 
#     @staticmethod
#     def _read_not_empty_rows_from_sheet(sheet):
# 
#         def _uenc(cell):
#             """ Encode unicode value to system encoding """
#             value = cell.value
#             if isinstance(value, basestring):
#                 return value.encode("utf8")
#             else:
#                 return value
# 
#         for ridx in xrange(sheet.nrows):
#             values = [_uenc(c) for c in sheet.row(ridx)]
#             if any(values):
#                 yield values
#             else:
#                 continue
# 
#     @classmethod
#     def unzip(cls, tab, row):
#         """ Unzip single zipped file in temporary local path """
#         dest_nfo = _get_dest_parts2(row.archname)
#         dest_path = dest_nfo.get("path")[1:] if dest_nfo.get("path").startswith("/") else dest_nfo.get("path")
#         tmp_path = dest_nfo.get("tmp_path")
#         if not tmp_path is None:
#             (__filename, stream) = tab.archive.retrieve(row.archive)
#             with zipfile.ZipFile(stream, "r") as adv:
#                 advfilenames = adv.namelist()
#                 # Da specifica l'archivio contiene un SOLO file
#                 assert len(advfilenames)==1, "Unsupported!"
#                 advfilename, ext = os.path.splitext(advfilenames[0])
#                 advfilename = "{fn}_{ts}.{ext}".format(
#                     fn = advfilename,
#                     ts = row.last_update.strftime("%Y%m%d-%H%M%S"),
#                     ext = ext
#                 )
# 
#                 mypath = os.path.join(tmp_path, dest_path)
#                 if not os.path.exists(mypath):
#                     os.makedirs(mypath)
# 
#                 adv.extract(advfilename, mypath)
#             ext_path = os.path.join(mypath, advfilename)
#             return ext_path
# 
#     @classmethod
#     def copy2(cls, tab, row, rename=True):
#         """ Copy result file in temporary local path """
#         dest_nfo = _get_dest_parts2(row.archname)
#         dest_path = dest_nfo.get("path")[1:] if dest_nfo.get("path").startswith("/") else dest_nfo.get("path")
#         tmp_path = dest_nfo.get("tmp_path")
#         if not dest_nfo.get("path") is None:
#             (filename, stream) = tab.archive.retrieve(row.archive)
#             if rename:
#                 dest_name = dest_nfo.get("name") or filename
#             else:
#                 dest_name = filename
#             dest_path = os.path.join(tmp_path, dest_path)
#             if not os.path.exists(dest_path):
#                 os.makedirs(dest_path)
#             dest_file_path = os.path.join(dest_path, dest_name)
#             shutil.copyfileobj(stream, open(dest_file_path, 'wb'))
# 
#             current.logger.debug("File %s successfully copied to: %s" % (dest_name, dest_path,))
#             return dest_file_path
# 
#     @staticmethod
#     def _get_refills():
#         """ Returns a dictionary with categories as keys and refills as values """
#         cpath = os.path.join(current.appconf.conf_price.path, "%(filename)s.%(extension)s" % current.appconf.conf_price)
#         def _iter():
#             with open(cpath, 'rb') as csvfile:
#                 spamreader = csv.reader(csvfile, delimiter=',')
#                 for row in spamreader:
#                     _row = List(*row)
#                     yield _row(0), _row(1)
# 
#         if os.path.isfile(cpath):
#             refills = dict([(k,v) for k,v in _iter()])
#         else:
#             refills = {}
#         return refills
# 
#     @classmethod
#     def _export_sheet_to_csv(cls, sheet, newcsvpath, priceCol=None, catCol="Categoria", osheet=None, pukcol=0):
#         """
#         sheet      @ADVSheetObject;
#         newcsvpath @string : Path where to save the sheet in a single csv file; 
#         priceCol   @string : Name of the column to which apply th refill;
#         catCol     @string : Name of the column of categories used to get the right refill.
#         WARNING: Only if priceCol is given it will try to apply refill.
#         """
# 
#         if not priceCol is None:
#             default_refill = current.appconf.conf_price.default_refill
#             refills = cls._get_refills()
#             _get_refill = lambda cat: float(refills.get(cat, 0)) or default_refill
# 
#         def _get_full_price(row):
#             refill = _get_refill(row[catCol])
# 
#             def _get_price():
#                 price = row[priceCol]
#                 if price in (None, "n.d.", '',):
#                     return None
#                 if isinstance(price, basestring):
#                     return float(price.replace(",", "."))
#                 else:
#                     return price
# 
#             raw_base_price = _get_price()
# 
#             if raw_base_price is None:
#                 return None
#             else:
#                 full_price = (1+refill/100.)*raw_base_price
#                 rounded_price = "%.2f" % full_price
#                 return rounded_price.replace(".", ",")
# 
#         new_col_head = "Prezzo Newirbel"
#         allvalues = cls._read_not_empty_rows_from_sheet(sheet)
#         fieldnames=allvalues.next()
#         if not priceCol is None:
#             fieldnames += [new_col_head]
# 
#         with open(newcsvpath, "w") as ADVcsv:
#             csvwriter = csv.DictWriter(ADVcsv, fieldnames=fieldnames)
#             csvwriter.writeheader()
#             for values in allvalues:
#                 csv_row_content = dict(zip(fieldnames, values))
#                 if not priceCol is None:
#                     full_price = _get_full_price(csv_row_content)
#                     if not full_price is None:
#                         csv_row_content[new_col_head] = full_price
#                 csvwriter.writerow(csv_row_content)
# 
#     @classmethod
#     def extract_tabs(cls, archname, filepath, removeOriginal=True,
#         priceCol = "Prezzo Base Rivenditore",
#         # Indicates the tab with the price column
#         priceTab = 0,
#         catCol = "Categoria",
#         # Extracts only the whole content of tab containing the price column (see priceTab)
#         priceOnly = True,
#         # filepath della eventuale vecchia versione del file
# #         ofp = None
#     ):
#         """ Split tabs of a single xls file into multiple csv """
# 
#         newcsvpaths = []
# 
#         with xlrd.open_workbook(filepath) as xlsSRC:
#             for sindex in xrange(xlsSRC.nsheets):
#  
#                 if not priceOnly or sindex == priceTab:
#  
#                     sheet = xlsSRC.sheet_by_index(sindex)
#                     newcsvpath = cls._get_new_path(archname, filepath, sindex)
#                     if not newcsvpath is None:
#                         if sindex == priceTab:
#                             cls._export_sheet_to_csv(sheet, newcsvpath, priceCol=priceCol, catCol=catCol)
#                         else:
#                             cls._export_sheet_to_csv(sheet, newcsvpath, priceCol=None)
#                         newcsvpaths.append(newcsvpath)
# 
#         if removeOriginal:
#             os.remove(filepath)
#             tmppath, _ = os.path.split(filepath)
#             try:
#                 os.removedirs(tmppath)
#             except OSError:
#                 pass
#             current.logger.info("File %s removed!" % filepath)
#         else:
#             current.logger.debug("File %s NOT removed for debug!" % filepath)
# 
#         return newcsvpaths
# 
#     @staticmethod
#     def _csvdiff(ofp, nfp):
#         """ """
#         priUIDName = "Codice Articolo"
#         destPath, _ = os.path.split(ofp)
#         
#         with open(ofp) as opri, open(nfp) as npri:
#             oprir = csv.DictReader(opri)
#             nprir = csv.DictReader(npri)
# 
#             orows = {row[priUIDName]: row for row in oprir}
#             nrows = {row[priUIDName]: row for row in nprir}
#             diffs = {k: v for k,v in nrows.iteritems() if v!=orows.get(k)}
# 
#             
# 
#     @classmethod
#     def run(cls, tab, row, removeTmp=False, orow=None):
#         # Prezzi
#         if row.archname == "arch_pri":
#             fp = cls.unzip(tab, row)
#             apaths = cls.extract_tabs(row.archname, fp, removeOriginal=removeTmp)
#             ofp = orow and cls.unzip(tab, orow)
#             if not ofp is None:
#                 opaths =  cls.extract_tabs(row.archname, ofp, removeOriginal=removeTmp)
#                 npaths = cls._csvdiff(apaths[0], opaths[0])
#                 return npaths
#             else:
#                 return apaths
#             
#         # Catalogo
#         elif row.archname == "arch_cat":
#             fp = cls.copy2(tab, row, rename=False)
#             return cls.extract_tabs(row.archname, fp, removeOriginal=removeTmp, priceCol=None)
#         # Altro
#         else:
#             return cls.copy2(tab, row, rename=True)

class DBSyncer(object):
    """ Retrieve files from remote FTP repository and store into DB """

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
        current.logger.info("File '%s' download started: %s" % (filename, prettydate(start),))
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

    def fetch(self, archname, source_path, period, name_starts=None, extension=None, force=False, **kw):
        success = False

        def _get_last_path(*path):
            _dir = path[-1]
            if _dir.endswith("YYYY"):
                dpath = os.path.join(*path[:-1])
                _dirs = tuple(sorted(filter(lambda n: n.startswith(_dir[:-4]), self.ftp.nlst(*dpath)), key=lambda m: m[-4:]))
                return os.path.join(*(path[:-1]+_dirs[-1:]))
            else:
                return os.path.join(*path)

#         now = datetime.datetime.now()

        rpath = source_path
        source_path = _get_last_path(*os.path.split(rpath))
        files = self.ftp.nlst(source_path)
#         nfiles = len(files)
        for __n,filename in enumerate(files):
            filepath = os.path.join(source_path, filename)

            if (name_starts is None or filename.lower().startswith(name_starts.lower())) \
                and (extension is None or filename.endswith(extension)):
                start = datetime.datetime.now()

                current.logger.info("SI!: %s - %s, %s" % (filepath, name_starts, extension,))

                if current.development:
                    self.ftp.sendcmd("TYPE i")
                    filesize = self.ftp.size(filepath)
                    self.ftp.sendcmd("TYPE A")
                    current.logger.debug("File: %s (size: %s)" % (filename, sizeof_fmt(filesize),))

                # --
                dbset = self.db(self.table.archname==archname)
                is_in_db = dbset.count()

#                 is_in_db = self.db(self.table.filename==filename).count()
                last_fs_update = datetime.datetime.strptime(self.ftp.sendcmd('MDTM ' + filepath)[4:], "%Y%m%d%H%M%S")

                if is_in_db==0:
                    current.logger.info("Downloading new file.")
                    newfilename, filehash = self.retrieve(filepath)
                    success = True
                    id = self.table.insert(
                        filename = filename,
                        archname = archname,
                        archive = newfilename,
                        last_update = last_fs_update,
                        checksum = filehash
                    )
                    row = self.table[id]
                    self.db.commit()
                else:
                    # --
                    row = dbset.select(limitby=(0,1)).first()
#                     row = self.db(self.db.archive.filename==filename).select(limitby=(0,1)).first()
                    if row.last_update < last_fs_update:
                        current.logger.info("Downloading updated file.")
                        newfilename, filehash = self.retrieve(filepath)
                        row.update_record(
                            filename = filename,
                            archive = newfilename,
                            last_update = last_fs_update,
                            checksum = filehash,
#                             is_active = True
                        )
                        self.db.commit()
                        success = True
                    else:
                        current.logger.info("It's time to update file but no new version found")

                current.logger.info("=== Fetching file operation terminated (Started: %s) ===\n" % prettydate(start))
            else:
                current.logger.debug("NO!: %s - %s, %s" % (filepath, name_starts, extension,))

        return success
