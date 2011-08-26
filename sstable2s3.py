#!/opt/ActivePython-2.7/bin/python
import boto
# import pyinotyify

from threading import Thread
from optparse import OptionParser
from StringIO import StringIO
import struct
import resource
import logging
import os.path
import socket
import json
import sys
import os
import time
import threading
import datetime
import math
import mimetypes
import hashlib
import io
import pickle
import sqlite3
import zlib
import binascii
from sstables3 import *


def main():
  # fix for http://bugs.python.org/issue7980 with strptime
  time.strptime('31 Jan 11', '%d %b %y')
  
  
  parser = OptionParser(usage='%prog [options] <bucket> <prefix> <path> <sqlite_db>')
  parser.add_option('-k', '--aws-key', dest='aws_key', default=None)
  parser.add_option('-s', '--aws-secret', dest='aws_secret', default=None)
  # TODO - if ignoring compacted files, don't upload them nor put them in the manifest files
  # parser.add_option('--ignore-compacted', action='store_true', dest='ignore_compacted', default=False)
  options, args = parser.parse_args()

  if len(args) < 4:
    parser.print_help()
    return -1

  bucket = args[0]
  prefix = args[1]
  path = args[2]
  sqlite = args[3]
  aws_key = options.aws_key
  aws_secret = options.aws_secret
  
  wrapper = SSTableS3(aws_key, aws_secret, bucket, prefix)
  wrapper.init_sqlite(sqlite)
  wrapper.sync_to_bucketPath(path)

if __name__ == '__main__':
  sys.exit(main())
