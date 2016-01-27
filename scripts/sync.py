# -*- coding: utf-8 -*-

# from multiprocessing import Pool
# 
# mypool = Pool(1)
# p.map(f, [1, 2, 3])

def main():
    for k in appconf.iterkeys():
        if k.startswith("ftp_"):
            res = fetchall(k)
            logger.info("Fetched %(len)s archives: %(fetched_archives)s" % res)

if __name__ == "__main__":
    main()