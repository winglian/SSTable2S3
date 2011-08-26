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


  manifest = args[2]
  path = args[3]
  sqlite = args[4]
  
  wrapper = SSTableS3(aws_key, aws_secret, bucket, prefix)
  local_filelist = wrapper.createPathManifest(path)
  
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

  # now loop through the compacted file prefixes and remove all the files related to the campacted sstables
  filtered_files = manifest_files
  for _filename in manifest_files:
    for file_prefix in compacted_list:
      if file_prefix in _filename:
        print 'found ' + file_prefix + ' in ' + _filename
        filtered_files.remove(_filename)
        break
      else:
        print 'NOT found ' + file_prefix + ' in ' + _filename
  print repr(manifest_files)
      
  # prompt the user which manifest to restore
  # prompt the user if they want to restore the compacted files too (save time by skipping them)
  # create the appropriate final directory structure
  # for each file in the manifest, break it down for each sstable and determine if there is a compacted file for it
  # see what files already exist locally in the path
  # copy down each file to a tmp directory
  # gunzip each file into the appropriate directories
  # set the correct permissions
  
if __name__ == '__main__':
  sys.exit(main())
