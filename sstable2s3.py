#!/opt/ActivePython-2.7/bin/python
import boto
# import pyinotyify

from threading import Thread
from optparse import OptionParser
import logging
import os.path
import socket
import json
import sys
import os
import time
import threading
import datetime

class CassS3Wrapper(object):
  def __init__(self, aws_key, aws_secret, bucket, key_prefix):
    self.aws_key = aws_key
    self.aws_secret = aws_secret
    self.bucket = bucket
    self.key_prefix = key_prefix
    s3 = boto.connect_s3(self.aws_key, self.aws_secret)
    self.bucket_obj = s3.get_bucket(bucket)
  
  def sync_to_bucketPath(self, path):
    # create a timestamped manifest file
    manifest = self.createPathManifest(path)
    # TODO upload the manifest file
    # list each file in the path
    # foreach file, check S3 - thread at this point
    for f in manifest:
      while True:
        if threading.activeCount() < 5:
          t = Thread(target=self.syncFileS3, args=(path, f))
          t.setDaemon(True)
          t.start()
          break
        else:
          time.sleep(1)

  def syncFileS3(self, pathhead, pathtail):
    # check if the file exists in S3
    # compare md5 or ts
    filepath = os.path.join(pathhead, pathtail)
    keyname = self.key_prefix + '/' + pathtail
    s3_key = self.bucket_obj.get_key(keyname)
    
    if s3_key:
      s3_datetime = datetime.datetime(*time.strptime(s3_key.last_modified, '%a, %d %b %Y %H:%M:%S %Z')[0:6])
      local_datetime = datetime.datetime.utcfromtimestamp(os.stat(filepath).st_mtime)
      s3_size = s3_key.size
      local_size = os.stat(filepath).st_size
      if local_datetime >= s3_datetime or s3_size != local_size:
        self.uploadFileS3(filepath, keyname, True)
    else:
      self.uploadFileS3(filepath, keyname, True)      

  def uploadFileS3(self, filepath, keyname, replace=False):
    def progress(sent, total):
      if sent == total:
        print 'Finished uploading ' + filepath + ' to ' + keyname
    
    key = self.bucket_obj.new_key(keyname)
    key.set_contents_from_filename(filepath, replace=replace, cb=progress)

  def createPathManifest(self, filepath):
    # print listFiles(filepath)
    # print os.listdir(filepath)
    lsr = self.listFiles(filepath)
    manifest = []
    for i in lsr:
      newpath = self.trimPath(i, filepath)
      if newpath.find('-tmp') == -1 and newpath.find('snapshots') == -1:
        manifest.append(newpath)
    return manifest
  
  def trimPath(self, path, relativeTo):
    # TODO - check if os.path.relpath() is available and use that
    # path = os.path.relpath(i, filepath)
    if path.startswith(relativeTo):
      replaced_path = path.replace(relativeTo, '', 1)
      if replaced_path.startswith('/'):
        replaced_path = replaced_path.replace('/', '', 1) 
      path = replaced_path
    return path

  def listFiles(self, path):
    files = []
    if (os.path.isdir(path) and not os.path.islink(path)):
      for i in os.listdir(path):
        filepath = os.path.join(path, i);
        if (os.path.isfile(filepath) and not os.path.islink(filepath)):
          files.append(filepath)
        else:
          files.extend(self.listFiles(os.path.join(path, i)))
    return files

def main():
  parser = OptionParser(usage='%prog [options] <bucket> <prefix> <path>')
  parser.add_option('-k', '--aws-key', dest='aws_key', default=None)
  parser.add_option('-s', '--aws-secret', dest='aws_secret', default=None)
  options, args = parser.parse_args()

  if len(args) < 3:
    parser.print_help()
    return -1

  bucket = args[0]
  prefix = args[1]
  path = args[2]
  aws_key = options.aws_key
  aws_secret = options.aws_secret
  
  wrapper = CassS3Wrapper(aws_key, aws_secret, bucket, prefix)
  wrapper.sync_to_bucketPath(path)

if __name__ == '__main__':
  sys.exit(main())
