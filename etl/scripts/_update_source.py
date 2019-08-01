# -*- coding: utf-8 -*-

import sys

from pandas import to_datetime
from ddf_utils.factory import CDIACLoader


source_dir = '../source/'
last_update = to_datetime('2018-02-01')


if __name__ == '__main__':
    cdiac = CDIACLoader()
    updated = cdiac.has_newer_source(last_update)
    if not updated:
        print('no newer source!')
        sys.exit(0)
    cdiac.bulk_download(source_dir)
