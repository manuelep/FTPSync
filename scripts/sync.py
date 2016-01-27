# -*- coding: utf-8 -*-

# from multiprocessing import Pool
# 
# mypool = Pool(1)
# p.map(f, [1, 2, 3])

def _clean():
    db.archive.truncate("CASCADE")
    try:
        shutil.rmtree(archive_upload_path)
    except:
        pass

def main():
    for k in appconf.iterkeys():
        if k.startswith("ftp_"):
            res = fetchall(k)
            logger.info("Fetched %(len)s archives: %(fetched_archives)s" % res)

if __name__ == "__main__":
    if 1 and current.development:
        _clean()
    main()