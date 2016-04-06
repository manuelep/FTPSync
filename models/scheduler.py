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

def fetchall(waits=0, force=False):

    source = appconf.source.source

    logger.debug("=== FETCHALL ===: Starting to fetching all data from: %s" % source)

    if waits>0:
        time.sleep(waits)

    last_update = db.archive.last_update.max()
    res = db(db.archive.id>0).select(
        db.archive.archname,
        last_update,
        groupby = db.archive.archname,
    )

    now = datetime.datetime.now()
    def checkrepo(archname):
        fres = res.find(lambda r: r.archive.archname==archname, limitby=(0,1,)).first()
        if fres is None:
            # se nessun download presente
            return True
        else:
            # se ultima modifica è troppo vecchia
            delta = (now-fres[last_update])
            return delta.total_seconds()>=appconf[archname]["period"]

    # downloadable archives
    archives = dict([(k,v) for k,v in appconf.iteritems() \
        if k.startswith('arch_') and v.ignore!=True and (force or checkrepo(k))
    ])

    if len(archives)>0:
        with Digger(table=db.archive, **appconf[source]) as oo:
            for k,nfo in archives.iteritems():
                current.logger.debug("Considering archive: %s" % k)
                oo.fetch(k, **nfo)
            oo.rsync()
    return {
        "fetched_archives": dict(archives),
        "len": len(archives),
    }

def rsync():
    Digger.rsync()

#scheduler = Scheduler(db, tasks=dict(fetchall=fetchall), migrate=appconf.db.migrate)
