# -*- coding: utf-8 -*-

class Fetcher(object):

    @staticmethod
    def _iter():
        for k,v in appconf.iteritems():
            yield k

    @classmethod
    def run(cls, db):
        for k in cls._iter():
            fetchall(db, k)


if __name__ == "__main__":
    Fetcher.run(db)