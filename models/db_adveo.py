# -*- coding: utf-8 -*-

from archiver import Elaborate, get_dest_parts
import csv, os

upload_rel_path = 'uploads/archive'
archive_upload_path = os.path.join(request.folder, upload_rel_path)

# File archive
db.define_table("archive",
    Field("filename", represent=lambda v,_: SPAN(v)),
    Field("archname"),
    Field("archive", "upload", uploadfolder=archive_upload_path, uploadseparate=True, autodelete=True),
    Field("last_update", "datetime"), # last update timestamp as resulting from remote filesystem analysis
    Field("checksum"),
    Field('keep', 'boolean', default=True),
    Field('is_active', 'boolean', writable=False, readable=False, default=True),
    auth.signature.created_on,
    auth.signature.modified_on,
    format = "%(filename)s"
#     common_filter = lambda query: db.archivio.is_active==True
)

db.archive.modified_on.readable = True
db.archive._enable_record_versioning()

class prepare(object):

    @staticmethod
    def products(removeOriginals=True):
        """ Merge informations from the last catalog, prices and refill files available """

        # 1. Extract last updated data from db

        repos = ("arch_cat", "arch_pri",)
        res = db(db.archive.archname.belongs(repos)).select(db.archive.ALL)

        assert len(res)==2, "ERROR!"
        grouped = res.group_by_value(db.archive.archname)

        # 2. Create working copy

        paths = {row.archname: Elaborate.run(db.archive, row, removeTmp=not current.development)[0] for row in res}

        def _mergeInfo():
            
            catUIDName = "Codice"
            priUIDName = "Codice Articolo"
            priceColName = "Prezzo Newirbel"
            outName = "Adveo_DB_WEB_Cat_MERGED.csv"
            
            mergingCols = ["Larghezza", "Profondità", "Altezza", "Peso", priceColName]
            mergingAdjustments = {
                # Weight adjustment (From gr to kg)
                "Peso": lambda v: ("%.3f" % (float(v)/1000)).replace(".", ",")
            }

            destPath, _ = os.path.split(paths["arch_cat"])
            outPath = os.path.join(destPath, outName)
            with open(paths["arch_cat"]) as cat, \
                open(paths["arch_pri"]) as pri, \
                open(outPath, "w") as res:

                # Readers
                catr = csv.DictReader(cat)
                prir = csv.DictReader(pri)
            
                # Rows by uuid
                cats = {row[catUIDName]: row for row in catr}
                pris = {row[priUIDName]: row for row in prir}
            
                def commons():
                    for cod in cats:
                        if cod in pris:
                            yield cod

                resw = csv.DictWriter(res, fieldnames=catr.fieldnames+mergingCols)

                resw.writeheader()
                for cod in commons():
                    row = dict(cats[cod], **{k: mergingAdjustments.get(k, lambda v: v)(pris[cod][k]) for k in mergingCols})
                    resw.writerow(row)

            if removeOriginals:
                os.remove(paths["arch_cat"])
                current.logger.info("File %s removed!" % paths["arch_cat"])
                os.remove(paths["arch_pri"])
                current.logger.info("File %s removed!" % paths["arch_pri"])
                tmpfolder, _ = os.path.split(paths["arch_pri"])
                try:
                    os.removedirs(tmpfolder)
                except OSError:
                    pass
            else:
                current.logger.debug("File %s NOT removed for debug!" % paths["arch_cat"])
                current.logger.debug("File %s NOT removed for debug!" % paths["arch_pri"])

            return outPath

        return _mergeInfo()