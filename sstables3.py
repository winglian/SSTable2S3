import zlib
import boto
import os
import json


MP_CHUNK_READ = 268435456 # 256MB
MAX_THREADS = 5


def write32u(output, value):
  output.write(struct.pack("<L", value))

class StreamCompressor:
  
  def __init__(self, compress_level=6):
    self.compressLevel = compress_level
    self.compressor = False
    self.size = 0
    self.crc = CRC_INIT
  
  def write(self, b):
    if not self.compressor:
      self.compressor = zlib.compressobj(self.compressLevel, zlib.DEFLATED, -zlib.MAX_WBITS, zlib.DEF_MEM_LEVEL, 0)
      self.size = 0
      self.crc = CRC_INIT
    zd = self.compressor.compress(b)
    self.size = self.size + len(b)
    self.crc = zlib.crc32(b, self.crc) & 0xffffffffL
    return zd
  
  def flush(self):
    zd = ''
    if self.compressor:
      zd = self.compressor.flush(zlib.Z_FULL_FLUSH);
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
#   def single_footer(self):
#   
#   def parts_footer(self):
#
#   def computeCRC(self):
# 

class MultiPartFileUploader():
  def __init__(self, filepath, bucket_name, key_name, aws_key, aws_secret, sqlite, chunk_size=MP_CHUNK_READ):
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
    self.chunk_size = chunk_size
    
  def checkExistingMPU(self):
    multipart_uploads = self.bucket_obj.list_multipart_uploads()
    for mpu in multipart_uploads:
      if (mpu.key_name == self.key_name):
        return mpu
    return False

  def getMissingParts(self):
    # stat the file size so we know how many parts we need
    file_size = self.fstats.st_size
    self.part_count = int(max(math.ceil(file_size / float(self.chunk_size)), 1))
    self.missing_part_ids = range(1,self.part_count+1)
    for uploaded_part in self.mpu.__iter__():
      sys.stderr.write(time.asctime() + ": " + self.key_name + " part " + str(uploaded_part.part_number) + " already uploaded with size " + str(uploaded_part.size) + "bytes\n")
      self.missing_part_ids.remove(uploaded_part.part_number)
    return self.missing_part_ids
  
  def uploadPart(self, part_number):
    # TODO -eventually upload using set_contents_from_stream rather than storing the whole part in memory
    d = io.BytesIO()
    with open(self.filepath, 'rb') as infd:
      # seek to position
      infd.seek((part_number-1)*self.chunk_size)
      bytes_remaining = self.chunk_size
      while bytes_remaining:
        bytes_to_read = min(bytes_remaining, 65536)
        # read data into buffer
        fdata = infd.read(bytes_to_read)
        bytes_remaining -= bytes_to_read
        d.write(fdata)
    self.mpu.upload_part_from_file(d, part_number)
    d.close()
  
  def uploadPartGz(self, part_number):
    # Filter.db files compress TOO well sometimes for the default 256MB chunk size and can be smaller than the 5MB minimum part size
    if ('Filter.db' in self.key_name):
      sc = StreamCompressor(1)
    else:
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
      seek_position = (part_number-1)*self.chunk_size
      infd.seek(seek_position)
      bytes_remaining = min(self.chunk_size, (self.fstats.st_size - seek_position))
      while bytes_remaining:
        bytes_to_read = min(bytes_remaining, 65536)
        # read data into buffer
        fdata = d.write(sc.write(infd.read(bytes_to_read)))
        bytes_remaining -= bytes_to_read
      # flush out any remaining data from the compressor
      d.write(sc.flush())
      # persist the crc and original chunk size for this part
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
    return crc
    
  def startUpload(self):
    # check which parts have already been uploaded
    missing_part_ids = self.getMissingParts()
    for part_num in missing_part_ids:
      sys.stderr.write(time.asctime() + ": Buffering part " + str(part_num) + "/" + str(self.part_count) + " for " + self.key_name + "\n")
      if self.key_name.endswith('.gz'):
        self.uploadPartGz(part_num)
      else:
        self.uploadPart(part_num)
      sys.stderr.write(time.asctime() + ": COMPLETED uploading part " + str(part_num) + "/" + str(self.part_count) + " for " + self.key_name + "\n")
    # recheck all the parts again before completing the upload
    missing_part_ids2 = self.getMissingParts()
    if len(missing_part_ids2)==0:
      self.mpu.complete_upload()
    else:
      sys.stderr.write(time.asctime() + ": INCOMPLETE upload due to missing parts for " + self.key_name + ", retry the next time around\n")

class SSTableS3(object):

  def __init__(self, aws_key, aws_secret, bucket, key_prefix):
    self.aws_key = aws_key
    self.aws_secret = aws_secret
    self.bucket = bucket
    # remove a trailing slash if it exists
    if key_prefix[-1:] == "/":
      key_prefix = key_prefix[0:-1]
    self.key_prefix = key_prefix
    self.connection = boto.connect_s3(self.aws_key, self.aws_secret)
    self.bucket_obj = self.connection.get_bucket(bucket)
  
  def init_sqlite(self, sqlite):
    self.sqlite = sqlite
    self.sqlite_connection = sqlite3.connect(sqlite)
    c = self.sqlite_connection.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS multipartuploads (key_name text, part_number integer, crc text, size integer, PRIMARY KEY (key_name, part_number))''')
    self.sqlite_connection.commit()

  def cancel_pending_uploads(self):
    multipart_uploads = self.bucket_obj.list_multipart_uploads()
    sys.stderr.write("Canceling outstanding multipart uploads\n")
    for mpu in multipart_uploads:
      if (self.key_prefix in mpu.key_name): # don't want to clobber things we shouldn't be handling
        sys.stderr.write("Canceling multipart upload for " + mpu.key_name + "\n")
        mpu.cancel_upload()
    sys.stderr.write("COMPLETED canceling outstanding multipart uploads\n")
    time.sleep(5)


  def sync_to_bucketPath(self, path):
    manifest = self.createPathManifest(path)
    manifest.sort()
    sys.stderr.write(str(len(manifest)) + " files in manifest\n")
    ts = time.time()
    key = self.bucket_obj.new_key('%s/manifests/%s-%s.manifest.json' % (self.key_prefix, socket.getfqdn(), ts))
    key.set_contents_from_string(json.dumps({'files': manifest, 'path': path, 'prefix': self.key_prefix, 'hostname': socket.getfqdn(), 'timestamp': ts}))
    threadlist = []
    for f in manifest:
      while True:
        if threading.activeCount() < MAX_THREADS:
          self.thread_wait = 0.015625
          # sys.stderr.write("starting new thread for " + f + " with " + str(threading.activeCount()) + "/" + str(MAX_THREADS) + " threads running\n")
          t = Thread(target=self.syncFileS3, args=(path, f))
          t.setDaemon(True)
          t.start()
          threadlist.append(t)
          break
        else:
          # sys.stderr.write("sleeping for " + str(self.thread_wait) + " seconds with " + str(threading.activeCount()) + "/" + str(MAX_THREADS) + " threads running\n")
          self.thread_wait = min(self.thread_wait * 2, 60);
          time.sleep(self.thread_wait)
    for t in threadlist:
      t.join()

  def syncFileS3(self, pathhead, pathtail):
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
        self.uploadFileS3(filepath, keyname, True)
    else:
      self.uploadFileS3(filepath, keyname, True)    

  def uploadFileS3(self, filepath, keyname, replace=False):
    file_size = os.stat(filepath).st_size

    # Filter.db files GZip TOO well. Upload as a single gz, not multipart upload or S3 will complain the parts are too small
    if ('Filter.db' in filepath):
      chunk_size = file_size
    else:
      chunk_size = MP_CHUNK_READ
    parts = int(max(math.ceil(file_size / float(chunk_size)), 1))
    
    sys.stderr.write(time.asctime() + ": Starting upload of " + filepath + " with " + str(parts) + " parts\n")
    start_time = time.clock()
    mpfu = MultiPartFileUploader(filepath, self.bucket, keyname, self.aws_key, self.aws_secret, self.sqlite, chunk_size)
    mpfu.startUpload()
    sys.stderr.write(time.asctime() + ": Finished upload of " + filepath + " with " + str(parts) + " parts\n")

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

  def listManifests(self):
    searchPrefix = self.key_prefix + '/manifests/'
    manifests = self.bucket_obj.get_all_keys(prefix=searchPrefix)
    return manifests

  def getManifest(self, manifest):
    key_obj = self.bucket_obj.get_key(manifest)
    manifest_contents = json.loads(key_obj.get_contents_as_string())
    return manifest_contents