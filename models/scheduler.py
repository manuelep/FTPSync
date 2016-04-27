# -*- coding: utf-8 -*-

from scheduler import Scheduler
from archiver import DBSyncer
import datetime, time
import shutil

def dbsync(waits=0, force=False):

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

    fetched = {}
    if len(archives)>0:
        with DBSyncer(table=db.archive, **appconf[source]) as oo:
            for k,nfo in archives.iteritems():
                current.logger.debug("Considering archive: %s" % k)
                success = oo.fetch(k, **nfo)
                if success:
                    fetched[k] = nfo
            #oo.rsync()
    return {
        "fetched_archives": fetched,
        "len": len(fetched),
    }

# def rsync():
#     Digger.rsync()

#scheduler = Scheduler(db, tasks=dict(fetchall=fetchall), migrate=appconf.db.migrate)
