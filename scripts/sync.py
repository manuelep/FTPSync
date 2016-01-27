# -*- coding: utf-8 -*-

# from multiprocessing import Pool
# 
# mypool = Pool(1)
# p.map(f, [1, 2, 3])

class Fetcher(object):

    @staticmethod
    def _iter():
        for k,v in appconf.iteritems():
            if k.startswith("ftp_"):
                yield k

    @classmethod
    def run(cls):

        for k in cls._iter():
#             _main = lambda t: fetchall(k, t)
#             mypool.map(_main, [0, 900, 1800, 5400])
            fetchall(k)

if __name__ == "__main__":
    Fetcher.run()