# -*- coding: utf-8 -*-

# from multiprocessing import Pool
# 
# mypool = Pool(1)
# p.map(f, [1, 2, 3])

from gluon.tools import fetch
import json
from archiver import rsync

def main():
    tmp_content = os.listdir(appconf.dest.tmp_path)
    if not tmp_content:
        try:
            res = dbsync(force=current.development)
        except Exception as err:
            logger.exeption("ERROR")
            raise err
            
        logger.info("Fetched %(len)s archives:\n\t%(fetched_archives)s" % {k: json.dumps(v, indent=4) for k,v in res.iteritems()})
        logger.info("=== End of fetching source! ===")

        mainarch = ("arch_cat", "arch_pri", "arch_avl", "arch_apt",)
   
        gorsync = False
        runproductupdate = False

        # Elaborazione congiunta degli archivi di catalogo, prezzi e disponibilità
        # per preparazione file dei prodotti (completo ed eventualmente quello parziale)
        if any([i in res["fetched_archives"] for i in mainarch]):
            try:
                prepare.products(updated=res["fetched_archives"].keys(), clean=not current.development)
            except Exception as err:
                logger.exeption("ERROR")
                raise err
            gorsync = True
            runproductupdate = True

        #elaborazioni singole (x archivi di immagini)
        otherarch =  [a for a in res["fetched_archives"] if not a in mainarch]
        if len(otherarch)>0:
            for row in db(db.archive.archname.belongs(otherarch)).select():
                prepare.getFromDB(row)                
                current.logger.info("File from %s archive copied to tmp destination." % db.archive)
            gorsync = True

        if gorsync:
            try:
                rsync()
            except Exception as err:
                logger.exeption("ERROR")
                raise err
 
        if runproductupdate and not current.appconf.trigger_url.ignore:
            try:
                html = fetch(current.appconf.trigger_url.url, current.appconf.trigger_data)
            except Exception as err:
                logger.exeption("ERROR")
                raise err
            current.logger.info(html)

    else:
        logger.error("Tmp folder is not empty: %s" % str(tmp_content))

if __name__ == "__main__":
    if current.development:
        current.logger.warning("WARNING! This script is running in DEV mode!!")
    main()
