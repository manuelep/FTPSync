# -*- coding: utf-8 -*-

def _clean():
    db.archive.truncate("CASCADE")
    db.commit()
#     if current.development:
#         for path in (archive_upload_path, appconf.misc.dest_path):        
#             try:
#                 shutil.rmtree(path)
#             except:
#                 pass

if __name__ == "__main__":
    _clean()
