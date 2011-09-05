#!/opt/ActivePython-2.7/bin/python
import boto

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
import re
from sstables3 import *

MAX_THREADS = 4

def main():
  # fix for http://bugs.python.org/issue7980 with strptime
  time.strptime('31 Jan 11', '%d %b %y')

  parser = OptionParser(usage='%prog [options] <bucket> <prefix> <manifest> <path>')
  parser.add_option('-k', '--aws-key', dest='aws_key', default=None)
  parser.add_option('-s', '--aws-secret', dest='aws_secret', default=None)
  parser.add_option('--restore-compacted', action='store_true', dest='restore_compacted', default=False)
  parser.add_option('--delete', action='store_true', dest='delete', default=False)
  options, args = parser.parse_args()

  aws_key = options.aws_key
  aws_secret = options.aws_secret
  restore_compacted = options.restore_compacted

  if len(args) >= 2:
    bucket = args[0]
    prefix = args[1]
    wrapper = SSTableS3(aws_key, aws_secret, bucket, prefix)
  else:
    parser.print_help()
    return -1
    
  
  if len(args)==2:
    # search the bucket for manifest files
    manifests = wrapper.listManifests()
    print repr(manifests)
    # echo out the manifest file listings
    return -1

  if len(args) < 4:
    parser.print_help()
    return -1

  manifest = args[2]
  target_path = args[3]
  
  wrapper = SSTableS3(aws_key, aws_secret, bucket, prefix)
  local_filelist = wrapper.createPathManifest(target_path)
  
  # download the requested manifest from s3
  manifest_data = wrapper.getManifest(manifest)
  # parse the manifests
  manifest_files = manifest_data['files']
  manifest_files.sort()

  if (restore_compacted == True):
    filtered_files = manifest_files
  else:
    filtered_files = wrapper.filterCompactedFiles(manifest_files)
      
  paths = []
  for _filename in filtered_files:
    # strip filename to last slash '/'
    last_slash_idx = _filename.rfind('/')
    _path = _filename[0:last_slash_idx]
    if _path not in paths:
      paths.append(_path)
  
  # create the appropriate final directory structure
  for _path in paths:
    fullpath = os.path.join(target_path, _path)
    if not os.path.exists(fullpath):
      os.makedirs(fullpath)
  
    
#     for f in manifest:
#       while True:
#         if threading.activeCount() < MAX_THREADS:
#           self.thread_wait = 0.015625
#           # sys.stderr.write("starting new thread for " + f + " with " + str(threading.activeCount()) + "/" + str(MAX_THREADS) + " threads running\n")
#           t = Thread(target=self.syncFileS3, args=(path, f))
#           t.setDaemon(True)
#           t.start()
#           threadlist.append(t)
#           break
#         else:
#           # sys.stderr.write("sleeping for " + str(self.thread_wait) + " seconds with " + str(threading.activeCount()) + "/" + str(MAX_THREADS) + " threads running\n")
#           self.thread_wait = min(self.thread_wait * 2, 60);
#           time.sleep(self.thread_wait)
#     for t in threadlist:
#       t.join()
  
  # see what files already exist locally in the path
  threadlist = []
  for _filename in filtered_files:
    fullpath = os.path.join(target_path, _filename)
    key = prefix
    if not prefix.endswith('/'):
      key = key + '/'
    key = key + _filename + '.gz'
    if os.path.exists(fullpath):
      print fullpath + ' already exists.. .skipping'
    else:
      print 'downloading ' + _filename + ' to ' + fullpath
      wrapper.downloadGzipFile(key, fullpath)
      wrapper.updateFileMtimeFromS3(key, fullpath)
      
  # copy down each file to a tmp directory
  # gunzip each file into the appropriate directories
  # set the correct permissions
  # delete files that aren't in manifest if 
  
if __name__ == '__main__':
  sys.exit(main())
