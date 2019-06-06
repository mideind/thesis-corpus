#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
    Module doc string
"""

import os
import time
import requests
import shutil


TIME_TIL_RETRY = 2
MAX_RETRY = 5


class Fetcher:

    _wait_before_next = True
    _time_last_fetched = time.time() - TIME_TIL_RETRY

    @classmethod
    def fetch_maybe(cls, url, path, save=False):
        """ Fetch from url or from file if it has been
            cached previously """
        if os.path.isfile(path):
            print("Found %s" % os.path.basename(path))
            with open(path, "rb") as file:
                return file.read(), True
        if save:
            return cls.fetch_and_save(url, path), False
        return cls.fetch_with_retry(url), False

    @classmethod
    def fetch_and_save(cls, url, path):
        """ Fetch file and save to disk """
        content = cls.fetch_with_retry(url)
        if not content:
            return False
        print("Saving {}".format(os.path.basename(path)))
        with open(path, "wb") as file:
            file.write(content)
        return content

    @classmethod
    def fetch_with_retry(cls, url):
        """ Fetch with retry """
        print("Fetching with retry...", url)
        for c in range(MAX_RETRY + 1):
            resp = cls.fetch(url)
            if resp:
                return resp.content
            if c < MAX_RETRY:
                print("Retrying...")
        return False

    @classmethod
    def fetch(cls, url):
        """ Fetch a single url """
        delta = time.time() - cls._time_last_fetched
        wait_time = TIME_TIL_RETRY - delta
        if wait_time > 0:
            time.sleep(wait_time)
        resp = requests.get(url)
        cls._time_last_fetched = time.time()
        resp.raise_for_status()
        return resp


def download_file(url, fname=None, dir_=None):
    """ https://stackoverflow.com/questions/16694907/download-large-file-in-python-with-requests """
    local_filename = url.split("/")[-1] if fname is None else fname
    r = requests.get(url, stream=True)
    local_file_path = (
        local_filename
        if dir_ is None
        else os.path.join(dir_, local_filename)
    )
    if dir_ is not None:
        os.makedirs(dir_, exist_ok=True)

    with open(local_file_path, "wb") as f:
        shutil.copyfileobj(r.raw, f)

    return local_file_path
