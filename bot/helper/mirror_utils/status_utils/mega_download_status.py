#!/usr/bin/env python3
from mega import MegaApi
from time import time
from bot.helper.ext_utils.status_utils import get_readable_file_size, MirrorStatus, get_readable_time


class MegaDownloadStatus:

    def __init__(self, name, size, gid, obj, message, listener):
        self._obj = obj
        self._name = name
        self._size = size
        self._gid = gid
        self._elapsed = time()
        #self.elapsed = get_readable_time(time() - self._elapsed)
        self.listener = listener
        self.message = message
        

    @staticmethod
    def engine():
        return 'Mega Sdk'

    def elapsed(self):
        return get_readable_time(time() - self._elapsed)
        
    def name(self):
        return self._name

    def progress_raw(self):
        try:
            return round(self._obj.downloaded_bytes / self._size * 100, 2)
        except:
            return 0.0

    def progress(self):
        return f"{self.progress_raw()}%"

    def status(self):
        return MirrorStatus.STATUS_DOWNLOADING

    def processed_bytes(self):
        return get_readable_file_size(self._obj.downloaded_bytes)

    def eta(self):
        try:
            seconds = (self._size - self._obj.downloaded_bytes) / \
                self._obj.speed
            return get_readable_time(seconds)
        except ZeroDivisionError:
            return '-'

    def size(self):
        return get_readable_file_size(self._size)

    def speed(self):
        return f'{get_readable_file_size(self._obj.speed)}/s'

    def gid(self):
        return self._gid

    def task(self):
        return self._obj
