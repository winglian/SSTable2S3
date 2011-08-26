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

CRC_INIT = zlib.crc32("") & 0xffffffffL

def main():
  # fix for http://bugs.python.org/issue7980 with strptime
  time.strptime('31 Jan 11', '%d %b %y')

  parser = OptionParser(usage='%prog [options] <bucket> <prefix> <manifest> <path>')
  parser.add_option('-k', '--aws-key', dest='aws_key', default=None)
  parser.add_option('-s', '--aws-secret', dest='aws_secret', default=None)
  parser.add_option('--restore-compacted', action='store_true', dest='restore_compacted', default=False)
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
  
  compacted_list = []
  # figure out the compacted files prefixes
  for _filename in manifest_files:
    if (_filename.endswith('-Compacted')):
      str_idx = _filename.rfind('-Compacted')
      compacted_list.append(_filename[0:str_idx+1])

  
  filtered_files = []
  
  if (restore_compacted == True):
    filtered_files = manifest_files
  else:
    # now loop through the compacted file prefixes and remove all the files related to the campacted sstables
    for _filename in manifest_files:
      found_match = False
      for file_prefix in compacted_list:
        if file_prefix in _filename:
          found_match = True
          break
      if not found_match:
        filtered_files.append(_filename)
  
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
  
  
  # see what files already exist locally in the path
  for _filename in filtered_files:
    fullpath = os.path.join(target_path, _filename)
    if os.path.exists(fullpath):
      print fullpath + ' already exists.. .skipping'
    else:
      print 'downloading ' + _filename + ' to ' + fullpath
      key = prefix
      if not prefix.endswith('/'):
        key = key + '/'
      key = key + _filename + '.gz'
      wrapper.downloadGzipFile(key, fullpath)
  # copy down each file to a tmp directory
  # gunzip each file into the appropriate directories
  # set the correct permissions
  
if __name__ == '__main__':
  sys.exit(main())
