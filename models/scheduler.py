# -*- coding: utf-8 -*-

from scheduler import Scheduler
from archiver import Digger
import datetime, time
import shutil

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

if current.development:
    db.archive.truncate("CASCADE")
    try:
        shutil.rmtree(archive_upload_path)
#         shutil.rmtree(os.path.join(request.folder, 'uploads/test'))
    except:
        pass

def fetchall(source="ftp_1", waits=0):

    if waits>0:
        time.sleep(waits)

    last_update = db.archive.last_update.max()
    res = db(db.archive.id>0).select(
        db.archive.archname,
        last_update,
        groupby = db.archive.archname,
    )

    def checkrepo(archname):
        fres = res.find(lambda r: r.archive.archname==archname, limitby=(0,1,)).first()
        if fres is None:
            # se nessun download presente
            return True
        else:
            # se ultima modifica è troppo vecchia
            delta = (datetime.datetime.now()-fres[last_update])
            return delta.total_seconds()>=appconf[archname]["period"]

    # downloadable archives
    archives = dict([(k,v) for k,v in appconf.iteritems() \
        if k.startswith('arch_') and v.get('source')==source and checkrepo(k)
    ])

    if len(archives)>0:
        with Digger(table=db.archive, **appconf[source]) as oo:
            for k,nfo in archives.iteritems():
                if nfo["source"] == source:
                    oo.fetch(k, **nfo)
    return {
        "fetched_archives": archives,
        "len": len(archives),
    }

#scheduler = Scheduler(db, tasks=dict(fetchall=fetchall), migrate=appconf.db.migrate)
