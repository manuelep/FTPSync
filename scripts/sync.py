# -*- coding: utf-8 -*-

# from multiprocessing import Pool
# 
# mypool = Pool(1)
# p.map(f, [1, 2, 3])
from gluon.tools import fetch
import json
from archiver import Elaborate

def main():
    tmp_content = os.listdir(appconf.dest.tmp_path)
    if not tmp_content:
        res = dbsync(force=current.development)
        logger.info("Fetched %(len)s archives:\n\t%(fetched_archives)s" % {k: json.dumps(v, indent=4) for k,v in res.iteritems()})
        logger.info("=== End of fetching source! ===")
        mainarch = ("arch_cat", "arch_pri",)

        gorsync = False
        runproductupdate = False

        # cat e pri necessitano di elaborazione congiunta
        if any([i in res["fetched_archives"] for i in mainarch]):
            prepare.products(removeOriginals=not current.development)
            gorsync = True
            runproductupdate = True

        # elaborazioni singole
        otherarch =  [a for a in res["fetched_archives"] if not a in mainarch]
        if len(otherarch)>0:
            for row in db(db.archive.archname.belongs(otherarch)).select():
                Elaborate.run(db.archive, row, removeTmp=not current.development)
                current.logger.info("File from %s archive copied to tmp destination." % db.archive)
            gorsync = True

        if gorsync:
            Elaborate.rsync()

        if runproductupdate:
            url = "http://newirbel.com/wp-cron.php?import_key=QDTB6K92&import_id=19&action=trigger"
            data = {"import_key": "QDTB6K92", "import_id": 19, "action": "trigger"}
            fetch(url, data)

    else:
        logger.error("Tmp folder is not empty: %s" % str(tmp_content))

if __name__ == "__main__":
    if current.development:
        current.logger.warning("WARNING! This script is running in DEV mode!!")
    main()