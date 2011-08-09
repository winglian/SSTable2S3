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

MP_CHUNK_READ = 100663296 # 96MB
CRC_INIT = zlib.crc32("") & 0xffffffffL

def write32u(output, value):
  output.write(struct.pack("<L", value))

class StreamCompressorOld:
  
  def __init__(self, filename, mode='rb', compresslevel=6, crc=CRC_INIT, sz=0):
    self.fileobj = open(filename, mode)
    self.compresslevel = compresslevel
    self.compressor = False
    # self.compressor = zlib.compressobj(compresslevel)
    self.crc = crc
    self.sz = sz
    
  def read(self, size=None):
    # read data
    if not self.compressor:
      self.compressor = zlib.compressobj(self.compresslevel, zlib.DEFLATED, -zlib.MAX_WBITS, zlib.DEF_MEM_LEVEL, 0)
    try:
      if size == None:
        fdata = self.fileobj.read()
      else:
        fdata = self.fileobj.read(size)
    finally:
      z_data = self.compressor.compress(fdata)
      self.crc = zlib.crc32(fdata, self.crc) & 0xffffffffL
      self.sz = self.sz + len(fdata)
      fz_data = self.finish()
    return z_data + fz_data
      
  def finish(self):
    return self.flush()

  def flush(self):
    fz_data = ''
    # fz_data = self.compressor.flush(zlib.Z_FULL_FLUSH);
    if self.compressor:
      fz_data = self.compressor.flush(zlib.Z_FULL_FLUSH);
    # self.compressor = False
    return fz_data

  def seek(self, offset):
    # seeks should reset the compressor as anything returned so far is invalid
    self.fileobj.seek(offset)
    # if self.compressor, then seek should error

  def tell(self):
    return self.fileobj.tell()

  # def close(self):
    # perform a Z_FULL_FLUSH so that this everything read so far can be 

class StreamCompressor:
  
  def __init__(self, compress_level=6):
    self.compressLevel = compress_level
#     self.stream = io.BytesIO()
    self.compressor = False
    self.size = 0
    self.crc = CRC_INIT
  
  def write(self, b):
    if not self.compressor:
      self.compressor = zlib.compressobj(self.compressLevel, zlib.DEFLATED, -zlib.MAX_WBITS, zlib.DEF_MEM_LEVEL, 0)
      self.size = 0
      self.crc = CRC_INIT
#     self.stream.seek(0, SEEK_END)
#     self.stream.write(self.compressor.compress(b))
    zd = self.compressor.compress(b)
    self.size = self.size + len(b)
    self.crc = zlib.crc32(b, self.crc) & 0xffffffffL
    return zd
  
  def flush(self):
    zd = ''
    if self.compressor:
      zd = self.compressor.flush(zlib.Z_FULL_FLUSH);
#     self.stream.seek(0, SEEK_END)
#     self.stream.write(zd)
    return zd
    
  def close(self):
    self.stream = False
    self.size = 0
    self.crc = CRC_INIT
  

class zlib_crc32():

  @staticmethod
  def gf2_matrix_square(square, mat):
    for n in range(0, 32):
      if (len(square) < (n + 1)):
        square.append(zlib_crc32.gf2_matrix_times(mat, mat[n]))
      else:
        square[n] = zlib_crc32.gf2_matrix_times(mat, mat[n])
    return square

  @staticmethod
  def gf2_matrix_times(mat, vec):
    sum = 0
    i = 0
    while vec:
      if (vec & 1):
        sum = sum ^ mat[i]
      vec = (vec >> 1) & 0x7FFFFFFF
      i = i + 1
    return sum

  @staticmethod
  def crc32_combine(crc1, crc2, len2):
    even = []
    odd = []
    if (len2 == 0): # degenerate case
      return crc1

    odd.append(0xEDB88320L) # CRC-32 polynomial
    row = 1

    for n in range(1, 32):
      odd.append(row)
      row = row << 1

    even = zlib_crc32.gf2_matrix_square(even, odd)
    odd = zlib_crc32.gf2_matrix_square(odd, even)

    while (len2 != 0):
      even = zlib_crc32.gf2_matrix_square(even, odd)
      if (len2 & 1):
        crc1 = zlib_crc32.gf2_matrix_times(even, crc1)
      len2 = len2 >> 1

      if (len2 == 0):
        break

      odd = zlib_crc32.gf2_matrix_square(odd, even)
      if (len2 & 1):
        crc1 = zlib_crc32.gf2_matrix_times(odd, crc1)
      len2 = len2 >> 1

    crc1 = crc1 ^ crc2
    return crc1

# class FileGz():
#   def __init__(self, filepath, keyname):
#     self.filepath = filepath
#     
#   def single_read(self):
#   
#   def part_read(self, part_num, seek, readsize):
#   
#   def header(self):
#     h = '\037\213' # magic header
#     h = h + '\010' # compression method
#     fname = os.path.basename(self.filepath)
#     flags = 0
#     if fname:
#       flags = 8 # FNAME (filename flag)
#     h = h + chr(flags)
#     mtime = self.fstats.st_mtime
#     h = h + struct.pack("<L", long(mtime))
#     h = h + '\002'
#     h = h + '\377'
#     if fname:
#       h = h + fname + '\000'
#     return h
#   
#   def single_footer(self)
#   
#   def parts_footer(self)
# 

class MultiPartFileUploader():
  def __init__(self, filepath, bucket_name, key_name, aws_key, aws_secret, sqlite):
    self.filepath = filepath
    self.fstats = os.stat(filepath)
    self.key_name = key_name
    self.bucket_name = bucket_name
    self.connection = boto.connect_s3(aws_key, aws_secret)
    self.bucket_obj = self.connection.get_bucket(bucket_name)
    self.part_count = 1
    self.crc_list = []
    # check existing multipart uploads for this key_name
    self.mpu = self.checkExistingMPU()
    if (self.mpu == False):
      self.mpu = self.bucket_obj.initiate_multipart_upload(key_name)
    self.sqlite = sqlite
    self.sqlite_connection = sqlite3.connect(sqlite)
    
  def checkExistingMPU(self):
    multipart_uploads = self.bucket_obj.list_multipart_uploads()
    for mpu in multipart_uploads:
      if (mpu.key_name == self.key_name):
        return mpu
    return False

  def getMissingParts(self):
    # stat the file size so we know how many parts we need
    file_size = self.fstats.st_size
    self.part_count = int(max(math.ceil(file_size / float(MP_CHUNK_READ)), 1))
    self.missing_part_ids = range(1,self.part_count+1)
    for part in self.mpu.__iter__():
      self.missing_part_ids.remove(part.part_number)
    return self.missing_part_ids
  
  def uploadPart(self, part_number):
    # TODO -eventually uplaod using set_contents_from_stream rather than storing the whole part in memory
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
        # incrementally calculate crc
    self.mpu.upload_part_from_file(d, part_number)
    d.close()
  
  def uploadPartGz(self, part_number):
    sc = StreamCompressor()
    d = io.BytesIO()
    # if it's the first part, let's write the gz header out
    if (part_number == 1):
      d.write('\037\213') # magic header
      d.write('\010') # compression method
      fname = os.path.basename(self.filepath)
      flags = 0
      if fname:
        flags = 8 # FNAME (filename flag)
      d.write(chr(flags))
      mtime = self.fstats.st_mtime
      write32u(d, long(mtime))
      d.write('\002')
      d.write('\377')
      if fname:
        d.write(fname + '\000')
  
    # then write out the gz data    
    with open(self.filepath, 'rb') as infd:
      # seek to position
      seek_position = (part_number-1)*MP_CHUNK_READ
      infd.seek(seek_position)
      bytes_remaining = min(MP_CHUNK_READ, (self.fstats.st_size - seek_position))
      while bytes_remaining:
        bytes_to_read = min(bytes_remaining, 65536)
        # read data into buffer
        fdata = d.write(sc.write(infd.read(bytes_to_read)))
        bytes_remaining -= bytes_to_read
        # incrementally calculate crc
      # flush out any remaining data from the compressor
      d.write(sc.flush())
      # save the crc and original chunk size for this part
      crc = sc.crc
      size = sc.size
      c = self.sqlite_connection.cursor()
      mpu_data = (self.key_name, part_number, format(crc, 'x'), size)
      c.execute('INSERT OR REPLACE INTO multipartuploads VALUES (?, ?, ?, ?)', mpu_data)
      self.sqlite_connection.commit()
      sc.close()
    
    # if it's the last part, write out the Z_FINISH flush '\x03\x00', cumulative crc, and original file size
    if (part_number == self.part_count):
      d.write('\x03\x00') # Z_FINISH, end the file  
      cumulative_crc = self.computeCumulativeCRC()
      write32u(d, cumulative_crc)
      write32u(d, self.fstats.st_size & 0xffffffffL)
    
    # upload the part now
    print "uploading " + self.key_name + " with data of length " # + str(len(d))
    upload_response = self.mpu.upload_part_from_file(d, part_number)
    d.close()
  
  def computeCumulativeCRC(self):
    part_number = 1
    crc = CRC_INIT
    # fetch crcs and sizes from sqlite
    c = self.sqlite_connection.cursor()
    k = [self.key_name]
    sql = 'SELECT part_number, crc, size FROM multipartuploads WHERE key_name=? ORDER BY part_number ASC'
    c.execute(sql, k)
    for row in c:
      pn = row[0]
      _crc = struct.unpack('>L', binascii.unhexlify(row[1].zfill(8)))[0]
      _size = row[2]
      crc = zlib_crc32.crc32_combine(crc, _crc, _size)
      
    # if (len(crc_list) != len(size_list): # throw some sort of error
#     for _crc, _size in zip(crc_list, size_list):
#       crc = zlib_crc32.crc32_combine(crc, _crc, _size)
#     print '0x%08x' % crc
    return crc
    
  def startUpload(self):
    # check which parts have already been uploaded
    missing_part_ids = self.getMissingParts()
    for part_num in missing_part_ids:
      print "Uploading part " + str(part_num) + " for " + self.key_name
      if self.key_name.endswith('.gz'):
        self.uploadPartGz(part_num)
      else:
        self.uploadPart(part_num)
    self.mpu.complete_upload()

class SSTable2S3(object):

  def __init__(self, aws_key, aws_secret, bucket, key_prefix, sqlite):
    self.aws_key = aws_key
    self.aws_secret = aws_secret
    self.bucket = bucket
    self.key_prefix = key_prefix
    self.connection = boto.connect_s3(self.aws_key, self.aws_secret)
    self.bucket_obj = self.connection.get_bucket(bucket)
    self.init_sqlite(sqlite)

#     multipart_uploads = self.bucket_obj.list_multipart_uploads()
#     for mpu in multipart_uploads:
#       print repr(mpu.get_all_parts())
#       for p in mpu.__iter__():
#         print str(p.part_number) + " : " + str(p.size) + "bytes"
#       # mpu.cancel_upload()
  
  def init_sqlite(self, sqlite):
    self.sqlite = sqlite
    self.sqlite_connection = sqlite3.connect(sqlite)
    c = self.sqlite_connection.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS multipartuploads (key_name text, part_number integer, crc text, size integer, PRIMARY KEY (key_name, part_number))''')
    self.sqlite_connection.commit()

  def sync_to_bucketPath(self, path):
    # create a timestamped manifest file
    print "preparing file manifest"
    manifest = self.createPathManifest(path)
    print str(len(manifest)) + " files in manifest"
    # TODO upload the manifest file
    # list each file in the path
    # foreach file, check S3 - thread at this point
    threadlist = []
    for f in manifest:
      while True:
        if threading.activeCount() < 5:
          self.thread_wait = 0.03125;
          print "starting new thread for " + f + " with " + str(threading.activeCount()) + " threads running"
          t = Thread(target=self.syncFileS3, args=(path, f))
          t.setDaemon(True)
          t.start()
          threadlist.append(t)
          break
        else:
          print "sleeping for " + str(self.thread_wait) + " seconds with " + str(threading.activeCount()) + " threads running"
          self.thread_wait = min(self.thread_wait * 2, 60);
          time.sleep(self.thread_wait)
    for t in threadlist:
      t.join()

  def syncFileS3(self, pathhead, pathtail):
    # compare md5 or ts
    filepath = os.path.join(pathhead, pathtail)
    if self.key_prefix.endswith('/'):
      keyname = self.key_prefix + pathtail + '.gz'
    else:
      keyname = self.key_prefix + '/' + pathtail + '.gz'
    connection = boto.connect_s3(self.aws_key, self.aws_secret)
    bucket_obj = connection.get_bucket(self.bucket)
    s3_key = bucket_obj.get_key(keyname)
    
    if s3_key:
      local_fstat = os.stat(filepath)
      s3_datetime = datetime.datetime(*time.strptime(s3_key.last_modified, '%a, %d %b %Y %H:%M:%S %Z')[0:6])
      local_datetime = datetime.datetime.utcfromtimestamp(local_fstat.st_mtime)
      s3_size = s3_key.size
      local_size = local_fstat.st_size
      # if local_datetime >= s3_datetime or s3_size != local_size:
      if local_datetime >= s3_datetime:
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
    
    parts = int(max(math.ceil(file_size / float(MP_CHUNK_READ)), 1))
    part_num = 0

    print "Uploading " + filepath + " using MULTI upload in " + str(parts) + " parts"
    start_time = time.clock()
    mpfu = MultiPartFileUploader(filepath, self.bucket, keyname, self.aws_key, self.aws_secret, self.sqlite)
    mpfu.startUpload()
    print 'Finished uploading ' + filepath + ' to ' + keyname + 'using MULTI upload in ' + str(start_time - time.clock()) + 's'

#     if file_size > MP_CHUNK_READ:
#       parts = math.ceil(file_size / MP_CHUNK_READ)
#       part_num = 0
#       # upload 5MB chunks to S3 in 
#       print "Uploading " + filepath + " using MULTI upload in " + str(parts) + " parts"
#       start_time = time.clock()
#       mpfu = MultiPartFileUploader(filepath, self.bucket, keyname, self.aws_key, self.aws_secret, self.sqlite)
#       mpfu.startUpload()
#       print 'Finished uploading ' + filepath + ' to ' + keyname + 'using MULTI upload in ' + str(start_time - time.clock()) + 's'
#     else:
#       print "Uploading " + filepath + " using single upload"
#       connection = boto.connect_s3(self.aws_key, self.aws_secret)
#       bucket_obj = connection.get_bucket(self.bucket)
#       key = bucket_obj.new_key(keyname)
#       key.set_contents_from_filename(filepath, replace=replace, cb=progress)

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
  
  
  parser = OptionParser(usage='%prog [options] <bucket> <prefix> <path> <sqlite_db>')
  parser.add_option('-k', '--aws-key', dest='aws_key', default=None)
  parser.add_option('-s', '--aws-secret', dest='aws_secret', default=None)
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
  
  wrapper = SSTable2S3(aws_key, aws_secret, bucket, prefix, sqlite)
  wrapper.sync_to_bucketPath(path)

if __name__ == '__main__':
  sys.exit(main())
