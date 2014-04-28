#!/usr/bin/env python

from collections import defaultdict
from errno import ENOENT
from stat import S_IFDIR, S_IFLNK, S_IFREG
from sys import argv, exit
from time import time

from fuse import FUSE, FuseOSError, Operations, LoggingMixIn

import cStringIO
import time
import dateutil.parser
import os

import threading 
import dropbox


uid = os.getuid()
gid = os.getgid()

class DropboxFS(LoggingMixIn, Operations):
	"""Example memory filesystem. Supports only one level of files."""

	def __init__(self, token):
		self.dropbox = dropbox.client.DropboxClient(token)
		self.data = defaultdict(str)
		self.fd = 0

		self.stats_cache = defaultdict(str)
		#the name lock was confliting with something from 
		self.mutex = threading.Lock()
		# The interval for cleaning up the cache
		cleanup_interval = 10
		threshold = 4
		self.stop = False
		self.cleanup_thread = threading.Thread(target = self.clean_cache, args =  (cleanup_interval, threshold, ) )
		#daemon set to true will make this thread terminate when other threads terminate, no need of sig handlers etc...
		self.cleanup_thread.daemon = True
		self.cleanup_thread.start()

	def clean_cache(self, delay=10, threshold=4):
		"""This is an internal loop to clean cached values where time is bigger than threshold"""		
		while(not self.stop):
			time.sleep(delay)						
			print 'running cleanup loop'		
			keys = self.stats_cache.keys()			
			for k in keys:
				with self.mutex:
					#Some items in the dictionary are empty, dont'y know why
					#nevertheless an exception would kill the clean-up thread we have to decide what to do					
					try:
						time_passed = time.time() - self.stats_cache[k]['time']							
						if (time_passed > threshold ):
							print 'removing item ', k, 'time passed ', time_passed
							self.stats_cache.pop(k)
					except Exception as ex:					
						print '-> error in item ', k, ':',  self.stats_cache[k]
						print ex
						# removes this item for the next loop
						self.stats_cache.pop(k)

	def get_metadata(self, path):
		try:
		    metadata = self.dropbox.metadata(path, list=False)
		except dropbox.rest.ErrorResponse as ex:
		    if ex.status == 404:
		        return None
		    else:
		        raise 
		else:
		    return metadata


	def set_cached_stats(self, path, stats):
		"""Adds an item to the cache"""
		with self.mutex:	
			self.stats_cache[path] = {'stats':stats, 'time':time.time()}

	def list_folder(self, path):
		# TODO: Change for 'delta' call to avoid 25,000 files limit
		metadata = self.dropbox.metadata(path, list=True)
		folders = []
		for entry in metadata['contents']:	
			stats = self.stats_from_metadata(entry)
			filename  = str(entry['path'])						
			self.set_cached_stats(filename, stats)
			folders.append(os.path.split(filename)[1])
		return folders

	def refresh_cache(self, path):
		"""Gets gets the metada of an item and writes it to the cache"""
		metadata = self.get_metadata(path)

		if not metadata:
			raise FuseOSError(ENOENT)

		stats = self.stats_from_metadata(metadata)
		self.set_cached_stats(path, stats)
		return stats

	def getattr(self, path, fh=None):
		with self.mutex:		
			if self.stats_cache[path]:
				time_passed = time.time() - self.stats_cache[path]['time']		
				if (time_passed < 3 ):					
					return self.stats_cache[path]['stats']
			
		print 'getting new stat for ', path		
		return self.refresh_cache(path)

	def stats_from_metadata(self, metadata):
		"""Converts dropbox metadata to stats"""
		if 'modified' in metadata:
			mtime = int(time.mktime(dateutil.parser.parse(metadata['modified']).timetuple()))
		else:
			mtime = int(time.time())

		if metadata['is_dir']:
			result = dict(st_mode=(S_IFDIR | 0755), st_nlink=1,
				st_size=metadata['bytes'], st_ctime=mtime, st_mtime=mtime, st_atime=mtime,
				st_uid=uid, st_gid=gid)
		else:
			result = dict(st_mode=(S_IFREG | 0755), st_nlink=1,
				st_size=metadata['bytes'], st_ctime=mtime, st_mtime=mtime, st_atime=mtime,
				st_uid=uid, st_gid=gid)

		return result
		
	def chmod(self, path, mode):
		return 0

	def chown(self, path, uid, gid):
		pass

	def create(self, path, mode):
		# TODO: 'mode' is ignored
		return self.open(path, os.O_CREAT | os.O_WRONLY | os.O_TRUNC)

	def destroy(self, path):
		pass

	def flush(self, path, fh):
		return 0

	def fsync(self, path, datasync, fh):
		return 0

	def fsyncdir(self, path, datasync, fh):
		return 0
	
	def getxattr(self, path, name, position=0):
		return ''

	def init(self, path):
		pass

	def link(self, target, source):
		self.dropbox.file_copy(source, target)

	def listxattr(self, path):
		return []

	def mkdir(self, path, mode):
		self.dropbox.file_create_folder(path)

	def mknod(self, path, mode, dev):
		raise FuseOSError(EROFS)

	def open(self, path, flags):
		try:
			f, metadata = self.dropbox.get_file_and_metadata(path)
		except dropbox.rest.ErrorResponse as ex:
			if ex.status == 404:
				rev = None
			else:
				raise
		else:
			rev = metadata['rev']

		if flags & os.O_CREAT or flags & os.O_TRUNC:
			nf = cStringIO.StringIO()
			self.dropbox.put_file(path, nf, overwrite=True, parent_rev=rev)
		elif rev:
			#CTM: This creates a readonly object
			#nf = cStringIO.StringIO(f.read())
			nf = cStringIO.StringIO()
			nf.write(f.read())

		else:
			raise FuseOSError(ENOENT)

		self.fd += 1
		#self.data[path] = {'f': nf, 'rev': rev}
		#now the key is the file handle, store the path just in case
		self.data[self.fd] = {'f': nf, 'rev': rev}
		return self.fd

	def opendir(self, path):
		self.fd += 1
		return self.fd

	def read(self, path, size, offset, fh):
		f = self.data[fh]['f']
		f.seek(offset)
		return f.read(size)

	def readdir(self, path, fh):
		return ['.', '..'] + self.list_folder(path)

	def readlink(self, path):
		with self.dropbox.get_file(path) as f:
		    return f.read()

	def release(self, path, fh):
		f = self.data[fh]['f']
		rev = self.data[fh]['rev']
		f.seek(0)
		self.dropbox.put_file(path, f, overwrite=True, parent_rev=rev)
		#We should remove the data from the dictionary at some point, so here seems nice
		# also we should get a way of re-using freed file handles
		self.data.pop(fh)
	
	

	def releasedir(self, path, fh):
		pass

	def removexattr(self, path, name):
		pass

	def rename(self, old, new):
		self.dropbox.file_move(old, new)

	def rmdir(self, path):
		self.dropbox.file_delete(path)

	def setxattr(self, path, name, value, options, position=0):
		pass

	def statfs(self, path):
		return dict(f_bsize=512, f_blocks=4096, f_bavail=2048)

	def symlink(self, target, source):
		self.dropbox.file_copy(source, target)

	def truncate(self, path, length, fh=None):
		f, metadata = self.dropbox.get_file_and_metadata(path)
		data = f.read(length)
		if len(data) < length:
		    pad = '\0' * (length - len(data))
		else:
		    pad = ''
		nf = cStringIO.StringIO(data + pad)
		self.dropbox.put_file(path, nf, overwrite=True, parent_rev=metadata['rev'])
		#self.refresh_cache(path)

	def unlink(self, path):
		pass

	def utimens(self, path, times=None):
		pass

	def write(self, path, data, offset, fh):
		f = self.data[fh]['f']
		f.seek(offset)
		print data
		f.write(data)
		#self.refresh_cache(path)
		return len(data)



if __name__ == "__main__":
    if len(argv) != 3:
        print 'usage: %s <token> <mountpoint>' % argv[0]
        exit(1)
    token = argv[1]
    mountpoint = argv[2]
    fuse = FUSE(DropboxFS(token), mountpoint, foreground=True)


