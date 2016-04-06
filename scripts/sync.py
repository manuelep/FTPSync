# -*- coding: utf-8 -*-

# from multiprocessing import Pool
# 
# mypool = Pool(1)
# p.map(f, [1, 2, 3])

def main():
    tmp_content = os.listdir(appconf.dest.tmp_path)
    if not tmp_content:
        res = fetchall(force=current.development)
        logger.info("Fetched %(len)s archives:\n%(fetched_archives)s" % res)
        logger.info("=== End of fetching source! ===")
    else:
        logger.error("Tmp folder is not empty: %s" % str(tmp_content))

if __name__ == "__main__":
    main()