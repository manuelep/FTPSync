#!/usr/bin/env python
# -*- coding: utf-8 -*-
from gluon import *
from gluon import current

from ftplib import FTP

import io, os

import hashlib

import datetime
from app.utils import prettydate
from gluon.dal import Row, Rows, Table

try:
    from cjson import decode as json_loads
except ImportError:
    current.logger.warning("For better performances please install cjson")
    from json import load as json_loads

import bz2

archive_upload_path = os.path.join(current.request.folder, 'uploads/archives')

archive_table_template = Table(None, "archivio",
    Field("filename", represent=lambda v,_: SPAN(v)),
    Field("archivio", "upload", uploadfolder=archive_upload_path, uploadseparate=True),
    Field("last_update", "datetime"), # last update timestamp as resulting from REMOTE filesystem analysis
    Field("checksum", readable=False, writable=False),
)

# TODO
# data_table_template = Table(None, "data",
#     Field("archivio_id", "reference archivio"),
#     Field("DATA_TIMESTAMP", "datetime", compute=Rawdata.get_timestamp, represent=lambda v,r: v.strftime("%Y-%m-%d %H:%M")),
#     Field("DATA_ADDRESS", compute=lambda r: Rawdata.get(r, "Address")),
#     Field("DATA_RAW_VALUE", compute=lambda r: Rawdata.get(r, "Value"), readable=False),
#     Field("DATA_VALUE", writable=False),
#     Field("raw_data", "json", writable=False, readable=False),
# )

class _Dummy(object):

    def __init__(self, db, tablename="dummy"):
        if tablename=="dummy" and not tablename in db.tables:
            db.define_table(tablename, archive_table_template)
        self.db = db
        self.tablename = tablename

class Ftp2DB(_Dummy):
    """ FTP to DB interface """

    def _download(self, oc, filename):
        """
        oc       @object : open FTP connection
        filename @string : name of file to download
        
        returns (newfilename, hex-md5-checksum,)
        """
        with io.BytesIO() as stream:
            start = datetime.datetime.now()
            callback = lambda c: stream.write(bz2.compress(c))
            oc.retrbinary('RETR ' + filename, stream.write, 1024)
            logger.info(T("File download started: %s") % prettydate(start, T))
            stream.seek(0)
            filehash = hashlib.new('sha224')
            filehash.update(stream.read())
            checksum = filehash.hexdigest()
            stream.seek(0)
            newfilename = self.db[self.tablename].archivio.store(stream, filename+".bz2")

        logger.debug("New file name: %s" % newfilename)
        logger.debug("checksum: %s" % checksum)

        return newfilename, checksum

    def fetch(self, url, user, passwd, limitby=None):
        """ """
        
        ftp = FTP(url)
        ftp.login(user=user, passwd=passwd)

        nlst = ftp.nlst() if limitby is None else ftp.nlst()[limitby[0]:limitby[1]]

        db = self.db
        for filename in ftp.nlst():
            logger.debug("Filename: %s" % filename)
            check = db(db[self.tablename].filename==filename).count()
            last_fs_update = datetime.datetime.strptime(ftp.sendcmd('MDTM ' + filename)[4:], "%Y%m%d%H%M%S")
            logger.debug("Files with identical name in archive: %s" % check)

            if check==0:
                # File surely needs download
                newfilename, filehash = _download(ftp, filename)
                logger.debug("Downloaded file: %s in %s" % (filename, newfilename,))
                db[self.tablename].insert(
                    filename = filename,
                    archivio = newfilename,
                    last_update = last_fs_update,
                    checksum = filehash
                )
            elif check==1:
                # First verify if last update timestamp is changed
                row = db(db[self.tablename].filename==filename).select(limitby=(0,1)).first()
                if row.last_update < last_fs_update:
                    newfilename, filehash = self._download(ftp, filename)
                    logger.warning("File %s seams neading update" % filename)
                    if filehash != row.checksum:
                        logger.warning("File %s content has been changed in remote archive!" % filename)
                        # Il file in archivio deve essere aggiornato
                        path_to_old_file = os.path.join(archive_upload_path, row.archivio)
                        # 1. Rimuovo il file su FS
                        os.remove(path_to_old_file)
                        # 2. Aggiorno record
                        row.update_record(
                            archivio = newfilename,
                            last_update = last_fs_update,
                            checksum = filehash
                        )
                        logger.info("ATTENZIONE! Il file in archivio remoto risulta aggiornato ed è stato scaricato nuovamente!")
                        logger.info("Filename: %s" % filename)
                    else:
                        # Il file già in archivio va bene così
                        path_to_new_file = os.path.join(archive_upload_path, newfilename)
                        os.remove(path_to_new_file)
                        logger.warning("Il file in archivio remoto risulta aggiornato rispetto a quello già archiviato ma gli hash risultano identici.")
                        logger.warning("Filename: %s" % filename)
                elif row.last_update > last_fs_update:
                    logger.error("Local file seams younger than remote")
                else:
                    logger.debug("File already in local archive.")
            else:
                logger.error("ATTENZIONE! Risultano più file in archivio con lo stesso nome e questo NON deve essere possibile!")
                assert False, "This should never happen, why it happens?"
    
        ftp.quit()
        db.commit()

class ArchLoader(_Dummy):
    """ Archive loader
    Library for data loading management from archive files.
    
    ###########################
    # PROGRESS                #
    ###########################
    
    """

    def _load_values(self, id, nfos):
        """
        id    @integer    : file archive id
        nfos  @ListOfDict : loaded json file content
        """
        ids = self.db[tablename].bulk_insert(map(lambda d: dict(archivio_id=id, raw_data=d), nfos))
        # DEPRECATED
#         _update_meta_info2(Rows(None, map(Row, nfos)).group_by_value("Address"))
        return ids

    @staticmethod
    def _load_reservations(id, nfos):
        """
        id    @integer    : file archive id
        nfos  @ListOfDict : loaded json file content
        """

        new_ids = []
        for nfo in nfos:
            RSRV_ID = JsonDrower('IDReservation').calculate_integer(nfo)
            new_id = db.prenotazione.update_or_insert(
                db.prenotazione.RSRV_ID==RSRV_ID,
                raw_data = nfo,
                archivio_id = id
            )
            if not new_id is None:
                new_ids.append(new_id)
            else:
                logger.debug("Reservation %s updated" % RSRV_ID)

        return new_ids

    @classmethod
    def _load(cls, filename, archivio_id, loaded_content):
        """
        filename       @string     : original filename archive
        archivio_id    @integer    : file archive id
        loaded_content @ListOfDict : loaded json file content
        """
        if filename.lower().endswith('values'):
            ids = cls._load_values(archivio_id, loaded_content)
        elif filename.lower().endswith('reservations'):
            ids = cls._load_reservations(archivio_id, loaded_content)
        else:
            ids = None
            logger.warning("File '%s' not recognized. By-passed." % row.filename)
        db = self.db
        db.commit()

        if not ids is None:
            if len(ids) > 0:
                logger.info("Inserted %s rows, from %s to %s" % (len(ids), ids[0], ids[-1],))
                if filename.lower().endswith('values'):
                    InfoView.refresh(db)
            else:
                logger.warning("Parsed file '%s' does not contain new data" % filename)

        return ids

    @classmethod
    def run(cls, id, row=None):
        
        if row is None:
            row = db.archivio[id].as_dict()
    
        (ufilename, stream) = db.archivio.archivio.retrieve(row['archivio'])
        datas = json_loads(bz2.decompress(stream.read()))
        filename, _ = row['filename'].split(os.extsep, 1) # # YYYY_MM_GG__xxxx.json.bz2
        cls._load(filename, id, datas)
