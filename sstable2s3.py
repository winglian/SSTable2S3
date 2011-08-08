#!/opt/ActivePython-2.7/bin/python
import boto
# import pyinotyify

from threading import Thread
from optparse import OptionParser
from StringIO import StringIO
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

MP_CHUNK_READ = 100663296 # 96MB

class MultiPartUploader:
  counter = 0
  def __init__(self, aws_key, aws_secret, bucket_name, key_name, buf_size=32768):
    self.key_name = key_name
    self.connection = boto.connect_s3(aws_key, aws_secret)
    self.bucket = self.connection.get_bucket(bucket_name)
    self.mp = self.bucket_obj.initiate_multipart_upload(key_name)
    print self.mp.get_all_parts()
    self.buf_size = buf_size
    self.buffer = StringIO()
    
  def write(self,s):
    self.buffer.write(s)
    if self.buffer.len >= self.buf_size:
      self.flush()
    
  def flush(self):
    if self.buffer.len:
      self.counter+=1
      self.mp.upload_part_from_file(self.buffer, self.counter)
      print "Uploaded Part " + str(self.counter) + " for " + self.key_name
      print resource.getrusage(resource.RUSAGE_SELF)
      self.buffer.close()
      self.buffer = StringIO()
  
  def close(self):
    self.flush()
    self.mp.complete_upload()
    

class MultiPartFileUploader():
  def __init__(self, filepath, bucket_name, key_name, aws_key, aws_secret):
    self.filepath = filepath
    self.key_name = key_name
    self.bucket_name = bucket_name
    self.connection = boto.connect_s3(aws_key, aws_secret)
    self.bucket_obj = self.connection.get_bucket(bucket_name)
    # check existing multipart uploads for this key_name
    self.mpu = self.checkExistingMPU()
    if (self.mpu == False):
      self.mpu = self.bucket_obj.initiate_multipart_upload(key_name)
  
  def checkExistingMPU(self):
    multipart_uploads = self.bucket_obj.list_multipart_uploads()
    for mpu in multipart_uploads:
      if (mpu.key_name == self.key_name):
        return mpu
    return False

  def getMissingParts(self):
    # stat the file size so we know how many parts we need
    file_size = os.stat(self.filepath).st_size
    parts = int(math.ceil(file_size / MP_CHUNK_READ))
    self.parts_missing = range(1,parts)
    for part in self.mpu.__iter__():
      print part.part_number
      self.parts_missing.remove(part.part_number)
    return self.parts_missing
  
  def uploadPart(self, part_number):
    # TODO -eventually uplaod using set_contents_from_stream rather than storing the whole part in memory
    m = hashlib.md5()
    d = io.BytesIO()
    #open the file
    with open(self.filepath, 'rb') as infd:
      # seek to position
      infd.seek((part_number-1)*MP_CHUNK_READ)
      bytes_remaining = MP_CHUNK_READ
      while bytes_remaining:
        bytes_to_read = min(bytes_remaining, 65536)
        # read data into buffer
        fdata = infd.read(bytes_to_read)
        bytes_remaining -= bytes_to_read
        d.write(fdata)
        # incrementally calculate md5
        m.update(fdata)
    self.mpu.upload_part_from_file(d, part_number)
    d.close()
    
  def startUpload(self):
    # check which parts have already been uploaded
    missing_parts = self.getMissingParts()
    for part_num in missing_parts:
      print "Uploading part " + str(part_num) + " for " + self.key_name
      self.uploadPart(part_num)
    

class SSTable2S3(object):

  def __init__(self, aws_key, aws_secret, bucket, key_prefix):
    self.aws_key = aws_key
    self.aws_secret = aws_secret
    self.bucket = bucket
    self.key_prefix = key_prefix
    self.connection = boto.connect_s3(self.aws_key, self.aws_secret)
    self.bucket_obj = self.connection.get_bucket(bucket)
    multipart_uploads = self.bucket_obj.list_multipart_uploads()
    for part in multipart_uploads:
      print part
      part_number_marker = 0
      while True:
        parts = part.get_all_parts(max_parts=1000, part_number_marker=part_number_marker)
        if parts:
          print parts
          part_number_marker += 1000
        else:
          break
    self.thread_wait = 1;
  
  def sync_to_bucketPath(self, path):
    # create a timestamped manifest file
    print "preparing file manifest"
    manifest = self.createPathManifest(path)
    print str(len(manifest)) + " files in manifest"
    # TODO upload the manifest file
    # list each file in the path
    # foreach file, check S3 - thread at this point
    for f in manifest:
      while True:
        if threading.activeCount() < 3:
          self.thread_wait = 0.03125;
          print "starting new thread for " + f + " with " + str(threading.activeCount()) + " threads running"
          t = Thread(target=self.syncFileS3, args=(path, f))
          t.setDaemon(True)
          t.start()
          break
        else:
          print "sleeping for " + str(self.thread_wait) + " seconds with " + str(threading.activeCount()) + " threads running"
          self.thread_wait = min(self.thread_wait * 2, 60);
          time.sleep(self.thread_wait)

  def syncFileS3(self, pathhead, pathtail):
    # compare md5 or ts
    filepath = os.path.join(pathhead, pathtail)
    if self.key_prefix.endswith('/'):
      keyname = self.key_prefix + pathtail
    else:
      keyname = self.key_prefix + '/' + pathtail
    connection = boto.connect_s3(self.aws_key, self.aws_secret)
    bucket_obj = connection.get_bucket(self.bucket)
    s3_key = bucket_obj.get_key(keyname)
    
    if s3_key:
      local_fstat = os.stat(filepath)
      s3_datetime = datetime.datetime(*time.strptime(s3_key.last_modified, '%a, %d %b %Y %H:%M:%S %Z')[0:6])
      local_datetime = datetime.datetime.utcfromtimestamp(local_fstat.st_mtime)
      s3_size = s3_key.size
      local_size = local_fstat.st_size
      if local_datetime >= s3_datetime or s3_size != local_size:
        print "starting upload - key " + keyname + " : incorrect filesize match or older timestamp in S3"
        self.uploadFileS3(filepath, keyname, True)
      else:
        print "skipping upload - key " + keyname + " ts and size match in S3"
    else:
      print "starting upload - key " + keyname + " does not exist yet in S3"
      self.uploadFileS3(filepath, keyname, True)    

  def uploadFileS3(self, filepath, keyname, replace=False):
    def progress(sent, total):
      if sent == total:
        print 'Finished uploading ' + filepath + ' to ' + keyname
    
    file_size = os.stat(filepath).st_size
    if file_size > 5242880:
      parts = math.ceil(file_size / MP_CHUNK_READ)
      part_num = 0
      # upload 5MB chunks to S3 in 
      print "Uploading " + filepath + " using MULTI upload in " + str(parts) + " parts"
      start_time = time.clock()
      mpfu = MultiPartFileUploader(filepath, self.bucket, keyname, self.aws_key, self.aws_secret)
      mpfu.startUpload()
#       mpu = MultiPartUploader(self.aws_key, self.aws_secret, self.bucket, keyname, MP_CHUNK_READ)
#       with open(filepath, 'rb') as infd:
#         while True:
#           instr = infd.read(65536)
#           if instr:
#             mpu.write(instr)
#           else:
#             break
#       mpu.close()
      print 'Finished uploading ' + filepath + ' to ' + keyname + 'using MULTI upload in ' + str(start_time - time.clock()) + 's'
    else:
      print "Uploading " + filepath + " using single upload"
      connection = boto.connect_s3(self.aws_key, self.aws_secret)
      bucket_obj = connection.get_bucket(self.bucket)
      key = bucket_obj.new_key(keyname)
      key.set_contents_from_filename(filepath, replace=replace, cb=progress)

  def createPathManifest(self, filepath):
    lsr = self.listFiles(filepath)
    manifest = []
    for i in lsr:
      newpath = self.trimPath(i, filepath)
      if newpath.find('-tmp') == -1 and newpath.find('snapshots') == -1:
        manifest.append(newpath)
    return manifest
  
  def trimPath(self, path, relativeTo):
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
  # fix for http://bugs.python.org/issue7980 with strptime
  time.strptime('31 Jan 11', '%d %b %y')
  
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
  
  wrapper = SSTable2S3(aws_key, aws_secret, bucket, prefix)
  wrapper.sync_to_bucketPath(path)

if __name__ == '__main__':
  sys.exit(main())
