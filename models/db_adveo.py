# -*- coding: utf-8 -*-

from archiver import get_dest_parts, get_dest_path, SheetReader, stock, Refiller, keep_fs_clean, load_duplicates
import csv, os, xlrd
import zipfile, shutil, hashlib

upload_rel_path = 'uploads/archive'
archive_upload_path = os.path.join(request.folder, upload_rel_path)

# File archive
db.define_table("archive",
    Field("filename", represent=lambda v,_: SPAN(v)),
    Field("archname"),
    Field("archive", "upload", uploadfolder=archive_upload_path, uploadseparate=True, autodelete=False),
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
db.archive_archive.archive.autodelete = True

class prepare(object):

    @staticmethod
    def _copy(stream, filename, destpath="/tmp"):
        """ """
        if not os.path.exists(destpath):
            os.makedirs(destpath)
        filepath = os.path.join(destpath, filename)
        shutil.copyfileobj(stream, open(filepath, 'wb'))
        current.logger.debug("File '%(filename)s' successfully copied to: %(destpath)s" % locals())
        return filepath

    @staticmethod
    def _splitxls(stream, filename, destpath="/tmp", destfilename=None, tabs=0):
        """ Split tabs of a single XLS file into multiple CSV files.
        Returns list of file paths.
        """

        if isinstance(tabs, int):
            tabs = (tabs,)

        def _getNewFilename(i):
            suffix = "_tab%s.csv" % i
            fn, __ext = os.path.splitext(destfilename or filename)
            return fn + suffix 

        def _loopOsheets():
            """ Loop over XLS file sheets """
            if filename.endswith("zip"):
                with zipfile.ZipFile(stream, "r") as cnt:
                    with xlrd.open_workbook(file_contents=cnt.read(cnt.namelist()[0])) as xlsSRC:
                        for sindex in (tabs or xrange(xlsSRC.nsheets)):
                            yield sindex, xlsSRC.sheet_by_index(sindex)
            else:
                with xlrd.open_workbook(file_contents=stream.read()) as xlsSRC:
                    for sindex in (tabs or xrange(xlsSRC.nsheets)):
                        yield sindex, xlsSRC.sheet_by_index(sindex)

        newcsvpaths = {}
        for sindex, sheet in _loopOsheets():
            newcsvpath = os.path.join(destpath, _getNewFilename(sindex))
            with open(newcsvpath, "w") as destcsv:
                mysreader = SheetReader(sheet)
                csvwriter = csv.DictWriter(destcsv, fieldnames=mysreader.header)
                csvwriter.writeheader()
                for r in mysreader():
                    csvwriter.writerow(r)
            newcsvpaths["tab%s" % sindex] = newcsvpath

        return newcsvpaths

    @staticmethod
    def _txt2csv(stream, filename, destpath="/tmp"):
        """ Convert stock text file into easier CSV file """
        newcsvpath = os.path.join(destpath, filename)
        with open(newcsvpath, "w") as destcsv:
            csvwriter = csv.DictWriter(destcsv, fieldnames=stock.header())
            csvwriter.writeheader()
            for r in stock.read(stream):
                csvwriter.writerow(r)
        return newcsvpath

    @classmethod
    def getFromDB(cls, row, destpath=None):
        """ """

        tab = db.archive

        dest_nfo = get_dest_parts(row.archname)
        filename, stream = tab.archive.retrieve(row.archive)

#         optt = {"destpath": "tmp_path", "destfilename": "name"}
#         optz = {k: dest_nfo[l] for k,l in optt.iteritems() if l in dest_nfo}

        destpath = destpath or get_dest_path(row.archname)
        destfilename = dest_nfo.get("name")
        if row.archname=="arch_cat":
            return cls._splitxls(stream, filename, destpath, destfilename, tabs=(0,1,))
        elif row.archname == "arch_pri":
            return cls._splitxls(stream, filename, destpath, destfilename)
        elif row.archname == "arch_avl":
            return {"tab0": cls._txt2csv(stream, destfilename or filename, destpath)}
        else:
            return cls._copy(stream, destfilename or filename, destpath)

    @classmethod
    def products(cls, updated=[], clean=True):
        """ Create files for update merging informations from last catalog,
        prices, refill and stock files available.
        Returns csv file path.
        updated @list : list of updated archives;
        clean   @bool : whether keep the fs cleaned or not;
        """

        outpath = get_dest_path("arch_cat")

        # 1. Extract last updated data from db
        repos = ("arch_cat", "arch_pri", "arch_avl",)
        assert db(db.archive.archname.belongs(repos)).count()==len(repos), "Error!"
        actuals = db(db.archive.archname.belongs(repos)).select().group_by_value(db.archive.archname)
        _actuals = db(db.archive.archname.belongs(repos))._select(db.archive.id)

        _olds = db(
            db.archive_archive.current_record.belongs(_actuals) & \
            db.archive_archive.archname.belongs(updated)
        )._select(
            db.archive_archive.id.max(),
            groupby = db.archive_archive.current_record
        )
        olds = db(db.archive_archive.id.belongs(_olds)).select().group_by_value(db.archive_archive.archname)

        apaths = {an: cls.getFromDB(rows[0]) for an,rows in actuals.iteritems()}
        opaths = {an: cls.getFromDB(rows[0], "/tmp") for an,rows in olds.iteritems()}

        duplicates = load_duplicates(apaths["arch_cat"]["tab1"])

        # 
        sortorder = [
           "arch_cat",
           "arch_avl",
           "arch_pri"
        ]

        # UID code columns
        UID = {
           "arch_cat": "Codice",
           "arch_pri": "Codice Articolo",
           "arch_avl": "Product SKU"
        }

        priCol = "Prezzo Base Rivenditore"
        catCol = "Categoria"
        fields = {
           "arch_cat": None, # That means ALL columns!
           "arch_pri": ("Larghezza", "Profondità", "Altezza", "Peso", catCol, priCol,),
           "arch_avl": ("Stock Quantity",)
        }

        # Value adjustments
        adjustments = {
            # Weight adjustment (From gr to kg)
            "Peso": lambda v: v and ("%.3f" % (float(v)/1000)).replace(".", ",")
        }

        # New computed columns
        refiller = Refiller()
        computeds = {
            "Prezzo Newirbel": lambda r: r[priCol] and ("%.2f" % refiller(r[catCol], float(r[priCol].replace(",", ".")))).replace(".", ","),
            "_UID": lambda r: hashlib.sha224("-".join([r[k] for k in (
                "Codice",
                "Nome Categoria Catalogo",
                "Nome Gruppo Catalogo Standard",
                "Nome Sottogruppo Catalogo standard",
            )]).lower()).hexdigest()
        }
        def _compute(row):
            for k,l in computeds.iteritems():
                yield k, l(row)

        # Rows by uuid for each archive
        # alailable contents VS old contents
        acnts, ocnts = {}, {}
        aflds = {}
        for an, nfo in apaths.iteritems():
            with open(nfo["tab0"]) as source:
                ardr = csv.DictReader(source)
                acnts[an] = {row[UID[an]]: row for row in ardr}
                if fields[an] is None:
                    aflds[an] = ardr.fieldnames
                else:
                    aflds[an] = fields[an]
                
            for an, nfo in opaths.iteritems():
                with open(nfo["tab0"]) as source:
                    ocnts[an] = {row[UID[an]]: row for row in csv.DictReader(source)}

        fieldnames = tuple(sum([list(aflds[k]) for k in sortorder], []) + computeds.keys())

        def _loopOduplicates(row, dpls):
            yield row
            for dpl in dpls:
                if any([v!=row[k] for k,v in dpl.iteritems()]):
                    rr = dict(row, **dpl)
                    rr = dict(rr, **{k: v for k,v in _compute(rr)})
                    yield {k: v for k,v in rr.iteritems() if k in fieldnames}

        _buildRow = lambda puk,k: acnts["arch_avl"][puk].get(k, acnts["arch_pri"][puk].get(k, acnts["arch_cat"][puk].get(k)))

        allfile = os.path.join(outpath, "all_product.csv")
        updfile = os.path.join(outpath, "updated_product.csv")
        with open(allfile, "w") as allprd, \
            open(updfile, "w") as updprd:

            # Create and initialize output csv files
            allw = csv.DictWriter(allprd, fieldnames=fieldnames)
            updw = csv.DictWriter(updprd, fieldnames=fieldnames)
            allw.writeheader()
            updw.writeheader()

            for puk in sorted(set.intersection(*map(set, acnts.values()))):

                arow = {k: _buildRow(puk, k) for k in fieldnames}
                orow = dict(sum(map(dict.items, [ocnts.get(an, {}).get(puk, {}) for an in sortorder]), []))
                
                for k,f in computeds.iteritems():
                    arow[k] = f(arow)
                    try:
                        orow[k] = f(orow)
                    except KeyError:
                        pass

                record_updated = any([v!=arow[k] for k,v in orow.iteritems() if k in fieldnames])

                for k,f in adjustments.iteritems():
                    arow[k] = f(arow[k])

                rows = [r for r in _loopOduplicates(arow, duplicates.get(arow[UID["arch_cat"]], [{}]))]
                map(allw.writerow, rows)
                if record_updated:
                    map(updw.writerow, rows)

        if clean:
            for paths in [apaths, opaths]:
                for __an, nfo in paths.iteritems():
                    for __, filepath in nfo.iteritems():
                        keep_fs_clean(filepath) 

        return [allfile, updfile]


#     @staticmethod
#     def productsOLD(removeOriginals=True):
#         """ DEPRECATED
#         Merge informations from the last catalog, prices and refill files available """
# 
#         # 1. Extract last updated data from db
# 
#         repos = ("arch_cat", "arch_pri", "arch_avl")
#         res = db(db.archive.archname.belongs(repos)).select()
#         opri = db(
#             db.archive_archive.current_record==res.find(lambda r: r.archname=="arch_pri").first().id
#         ).select(
#             orderby = ~db.archive_archive.modified_on,
#             limitby = (1,0,)
#         ).first()
# 
#         assert len(res)==2, "ERROR!"
#         grouped = res.group_by_value(db.archive.archname)
# 
#         # 2. Create working copy
# 
#         paths = {row.archname: Elaborate.run(db.archive, row, removeTmp=not current.development, orow=opri)[0] for row in res}
# 
#         def _mergeInfo():
#             
#             catUIDName = "Codice"
#             priUIDName = "Codice Articolo"
#             priceColName = "Prezzo Newirbel"
#             outName = "Adveo_DB_WEB_Cat_MERGED.csv"
#             
#             mergingCols = ["Larghezza", "Profondità", "Altezza", "Peso", priceColName]
#             mergingAdjustments = {
#                 # Weight adjustment (From gr to kg)
#                 "Peso": lambda v: ("%.3f" % (float(v)/1000)).replace(".", ",")
#             }
# 
#             destPath, _ = os.path.split(paths["arch_cat"])
#             outPath = os.path.join(destPath, outName)
#             with open(paths["arch_cat"]) as cat, \
#                 open(paths["arch_pri"]) as pri, \
#                 open(outPath, "w") as res:
# 
#                 # Readers
#                 catr = csv.DictReader(cat)
#                 prir = csv.DictReader(pri)
#             
#                 # Rows by uuid
#                 cats = {row[catUIDName]: row for row in catr}
#                 pris = {row[priUIDName]: row for row in prir}
#             
#                 def commons():
#                     for cod in cats:
#                         if cod in pris:
#                             yield cod
# 
#                 resw = csv.DictWriter(res, fieldnames=catr.fieldnames+mergingCols)
# 
#                 resw.writeheader()
#                 for cod in commons():
#                     row = dict(cats[cod], **{k: mergingAdjustments.get(k, lambda v: v)(pris[cod][k]) for k in mergingCols})
#                     resw.writerow(row)
# 
#             if removeOriginals:
#                 os.remove(paths["arch_cat"])
#                 current.logger.info("File %s removed!" % paths["arch_cat"])
#                 os.remove(paths["arch_pri"])
#                 current.logger.info("File %s removed!" % paths["arch_pri"])
#                 tmpfolder, _ = os.path.split(paths["arch_pri"])
#                 try:
#                     os.removedirs(tmpfolder)
#                 except OSError:
#                     pass
#             else:
#                 current.logger.debug("File %s NOT removed for debug!" % paths["arch_cat"])
#                 current.logger.debug("File %s NOT removed for debug!" % paths["arch_pri"])
# 
#             return outPath
# 
#         return _mergeInfo()
