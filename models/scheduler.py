# -*- coding: utf-8 -*-

import os
from scheduler import Scheduler
from archiver import Digger
import datetime

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

def fetchall(source="ftp_1"):

    lastedit = db.archive.modified_on.max()
    res = db(db.archive.id>0).select(
        db.archive.archname,
        lastedit,
        groupby = db.archive.archname,
    )

    def checkrepo(archname):
        fres = res.find(lambda r: r.archive.archname==archname, limitby=(0,1,)).first()
        if fres is None:
            # se nessun download presente
            return True
        else:
            # se ultimo upload è troppo vecchio
            delta = (datetime.datetime.now()-fres[lastedit])
            return delta.total_seconds()>=appconf[archname]["period"]

    # downloadable archives
    archives = dict([(k,v) for k,v in appconf.iteritems() \
        if k.startswith('arch_') and v.get('source')==source and checkrepo(k)
    ])

    if len(archives)>0:
        with Digger(table=db.archive, **appconf.ftp_1) as oo:
            for k,nfo in archives.iteritems():
                if nfo["source"] == source:
                    oo.fetch(k, **nfo)
    return {
        "fetched_archives": archives,
        "len": len(archives),
    }

scheduler = Scheduler(db, tasks=dict(fetchall=fetchall), migrate=appconf.db.migrate)
