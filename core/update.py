# -*- coding: utf-8 -*-


import os
import sys
from log import *

_SOURCE_FORMAT = ".js"
_TARGET_FORMAT = ".txt"


class UpdateTxt(object):
    def __init__(self):
        pass

    @classmethod
    def transfer(cls, source_folder, target_folder, is_copy=True, is_delete_souece=False):
        assert isinstance(source_folder, basestring)
        assert isinstance(target_folder, basestring)

        log.info("start js data copy to txt data")
        if is_copy:
            import shutil
            if os.path.exists(target_folder):
                log.info("%s is exist, please reinput other a new folder." % target_folder)
                sys.exit(1)
            log.info("working copy %s" % source_folder)
            shutil.copytree(source_folder, target_folder)

        for dirpath, _, filenames in os.walk(target_folder):
            for filename in filenames:
                if filename.endswith(_SOURCE_FORMAT):
                    cur_file_name = os.path.splitext(filename)[0]
                    cur_file = os.path.join(dirpath, filename)
                    new_file = os.path.join(dirpath, (cur_file_name + _TARGET_FORMAT))
                    try:
                        os.renames(cur_file, new_file)
                    except IOError as e:
                        log.error("os.renames occur IOError: %s" % e.message)
                    except Exception as e:
                        log.error("os.renames occur Exception: %s" % e.message)

        if is_delete_souece:
            import shutil
            shutil.rmtree(source_folder)

        log.info("end js data copy to txt data")
