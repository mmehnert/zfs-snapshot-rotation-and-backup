'''
Created on 4 Sep 2012

@author: Maximilian Mehnert <maximilian.mehnert@gmx.de>
'''

import subprocess
import datetime
import signal
import time
import pprint
pp = pprint.PrettyPrinter(indent=4)

class ZFS_iterator:
	verbose=False
	dry_run=False
	pool=None
	i = 0

	def __init__(self,pool):
		self.pool=pool
		self.i = 0

	def __iter__(self):
			return self

	def next(self):
		if self.i < len(self.pool.zfs_filesystems):
			i = self.i
			self.i += 1
			origin=self.pool.get_origin(fs=self.pool.zfs_filesystems[i])
			if origin != "-":
				origin=origin.split('@')[0]
				a, b = self.pool.zfs_filesystems.index(self.pool.zfs_filesystems[i]),\
					self.pool.zfs_filesystems.index(origin)
				if a < b:
					self.pool.zfs_filesystems[b], self.pool.zfs_filesystems[a] = \
						self.pool.zfs_filesystems[a], self.pool.zfs_filesystems[b]
			return self.pool.zfs_filesystems[i]
		else:
			raise StopIteration()

	def __iter__(self):
			return self

	def next(self):
		if self.i < len(self.pool.zfs_filesystems):
			i = self.i
			self.i += 1
			return self.pool.zfs_filesystems[i]
		else:
			raise StopIteration()

class ZFS_pool:
	verbose=False
	dry_run=False
	remote_cmd=""
	pool=""
	zfs_filesystems=[]
	zfs_snapshots=[]

	def __init__(self,pool,remote_cmd="",verbose=False,dry_run=False):
		self.pool=pool
		self.remote_cmd=remote_cmd
		self.update_zfs_filesystems()
		self.update_zfs_snapshots()
		self.verbose=verbose
		self.dry_run=dry_run

	def update_zfs_snapshots(self, timeout=180):
		with TimeoutObject(timeout):
			waitfor_cmd_to_exit(remote=self.remote_cmd, cmd_line_parts=["zfs","list","snapshot"], sleep=5)
		snapshot_list=subprocess.check_output(self.remote_cmd+" zfs list -o name -t snapshot -H -r "+\
			self.pool, shell=True,universal_newlines=True).split("\n")
		self.zfs_snapshots=snapshot_list
		return snapshot_list

	def update_zfs_filesystems(self):
		fs_list=subprocess.check_output(self.remote_cmd+' zfs list -o name -H -r '+self.pool,shell=True,universal_newlines=True).split("\n")
		fs_list=fs_list[0:-1]
		i=0
		while i < len(fs_list):
			origin=self.get_origin(fs_list[i])
			if origin != "-":
				origin=origin.split('@')[0]
				a, b = fs_list.index(fs_list[i]), fs_list.index(origin)
				if a < b:
					fs_list[b], fs_list[a] = fs_list[a], fs_list[b]
			i += 1
		self.zfs_filesystems=fs_list
		return self.zfs_filesystems

	def get_zfs_snapshots(self,fs="", recursive=False):
		match=fs if recursive else fs+"@"
		for snapshot in self.zfs_snapshots:
			if snapshot.startswith(match):
				yield snapshot

	def get_zfs_snapshots_reversed(self,fs="", recursive=False):
		match=fs if recursive else fs+"@"
		for snapshot in reversed(self.zfs_snapshots):
			if snapshot.startswith(match):
				yield snapshot

	def __iter__(self):
		return ZFS_iterator(self)

	def get_origin(self,fs=None):
		origin=subprocess.check_output(self.remote_cmd+' zfs get origin '+fs,shell=True,universal_newlines=True).split()
		origin=origin[6:7][0]
		return origin

	def get_zfs_filesystems(self, fs_filter=""):
		for fs in self.zfs_filesystems:
			if fs.startswith(fs_filter):
				yield fs

	def sort_for_destruction(self,fs_filter=""):
		zfs_fs=list(self.zfs_filesystems)
		for fs in zfs_fs:
			fs_parts=fs.split("/")
			if len(fs_parts) > 1:
				parent="/".join(fs_parts[0:len(fs_parts)-1])
				parentIdx=zfs_fs.index(parent)
				if parentIdx < zfs_fs.index(fs):
					zfs_fs.remove(fs)
					zfs_fs.insert(parentIdx,fs)
		for fs in zfs_fs:
			origin=self.get_origin(fs=fs)
			if origin != "-":
				origin=origin.split('@')[0]
				zfs_fs.remove(fs)
				originIdx=zfs_fs.index(origin)
				zfs_fs.insert(originIdx,fs)
		for fs in zfs_fs:
			if fs.startswith(fs_filter):
				yield fs

	def scrub_running():
		zfs_output=subprocess.check_output(self.remote_cmd+" zpool status "+pool.pool, shell=True)
		return  "scrub in progress" in zfs_output

class ZFS_fs:
	fs=None
	pool=None

	def __init__(self,fs=None,remote_cmd="", pool=None, verbose=False, dry_run=False):
		self.verbose=verbose
		self.dry_run=dry_run
		if fs==None:
			raise ValueError("No filesystem specified")
		else:
			self.fs=fs
		if pool==None:
			self.pool=ZFS_pool(fs.split("/")[0],remote_cmd=remote_cmd)
		else:
			self.pool=pool

	def get_snapshots(self):
		return self.pool.get_zfs_snapshots(fs=self.fs, recursive=False)

	def get_snapshots_reversed(self):
		return self.pool.get_zfs_snapshots_reversed(fs=self.fs, recursive=False)

	def get_last_snapshot(self):
		ss=subprocess.check_output(
			self.pool.remote_cmd+" zfs list -o name -t snapshot -H -r "+self.fs+" |grep ^"+\
				self.fs+"@",shell=True,universal_newlines=True).split("\n")[-2]
		return ss

	def get_first_snapshot(self):
		ss=subprocess.check_output(
			self.pool.remote_cmd+" zfs list -o name -t snapshot -H -r "+self.fs+" |grep ^"+\
				self.fs+"@",shell=True,universal_newlines=True).split("\n")[0]
		return ss

	def get_last_common_snapshot(self,dst_fs=None):
		for dst_snapshot in dst_fs.get_snapshots_reversed():
			for src_snapshot in self.get_snapshots_reversed():
				if dst_snapshot.split('@')[1] == src_snapshot.split('@')[1]:
					return src_snapshot
		return None

	def create_zfs_snapshot(self,prefix=""):
		if len(prefix)==0:
			raise ValueError("prefix for snapshot must be defined")
		snapshot=self.fs+"@"+prefix+"-"+self.timestamp_string()
		snapshot_command=self.pool.remote_cmd+" zfs snapshot "+snapshot
		if self.verbose or self.dry_run:
			print("Running: "+snapshot_command)
		if not self.dry_run:
			subprocess.check_call(snapshot_command, shell=True)
			self.pool.zfs_snapshots.append(snapshot)
			return snapshot

	def transfer_to(self,dst_fs=None):
		if self.verbose:
			print("trying to transfer: "+self.pool.remote_cmd+" "+self.fs+" to "+dst_fs.pool.remote_cmd+" "+dst_fs.fs+".")

		if dst_fs.fs in dst_fs.pool.zfs_filesystems:
			print(dst_fs.fs+" already exists.")
			pass
		else:
			last_src_snapshot=self.get_last_snapshot()
			first_src_snapshot=self.get_first_snapshot()
			if self.verbose:
				print("found "+last_src_snapshot)
			command=self.pool.remote_cmd+" zfs send -p -I "+first_src_snapshot+" "+last_src_snapshot+" | "+dst_fs.pool.remote_cmd+" zfs receive "+dst_fs.fs
			if self.verbose or self.dry_run:
				print("running "+command)
			if not self.dry_run:
				subprocess.check_call(command,shell=True)
			return True


	def sync_with(self,dst_fs=None,target_name=""):
		if self.verbose:
			print("Syncing "+self.pool.remote_cmd+" "+self.fs+" to "+dst_fs.pool.remote_cmd+" "+dst_fs.fs+" with target name "+target_name+".")

		if dst_fs.fs in dst_fs.pool.zfs_filesystems:
			last_common_snapshot=self.get_last_common_snapshot(dst_fs=dst_fs)
		else:
			if self.verbose:
				print(dst_fs.pool.remote_cmd+" "+dst_fs.fs+" does not exist.")
			last_common_snapshot=None

		if last_common_snapshot != None:
			sync_mark_snapshot=self.create_zfs_snapshot(prefix=target_name)
			if self.verbose:
				print("Sync mark created: "+self.pool.remote_cmd+" "+sync_mark_snapshot)

			dst_fs.rollback(last_common_snapshot.split("@")[1])
			return self.run_sync(dst_fs=dst_fs,start_snap=last_common_snapshot,
				stop_snap=sync_mark_snapshot)

		else:
			self.create_zfs_snapshot(prefix=target_name)
			return self.transfer_to(dst_fs=dst_fs)

	def run_sync(self,dst_fs=None, start_snap=None, stop_snap=None):
		sync_command=self.pool.remote_cmd+" zfs send -p -I "+start_snap+" "+stop_snap+ "|"+\
			dst_fs.pool.remote_cmd+" zfs receive "+dst_fs.fs
		if self.verbose or self.dry_run:
			print("Running sync: "+sync_command)
		if not self.dry_run:
			subprocess.check_call(sync_command,shell=True)

			dst_fs.pool.update_zfs_snapshots()
			sync_mark=stop_snap.split("@")[1]
			for snap in dst_fs.get_snapshots():
				if snap.split("@")[1]==sync_mark:
					if self.verbose:
						print("Sucessfully transferred "+stop_snap)
					return True

	def rollback(self,snapshot):
		rollback=self.pool.remote_cmd+" zfs rollback -r "+self.fs+"@"+snapshot
		if self.dry_run==True:
			print(rollback)
			return True
		else:
			if self.verbose:
				print("Running rollback: "+rollback)
			subprocess.check_call(rollback,shell=True)

	def clean_snapshots(self,prefix="", number_to_keep=None):
		if self.verbose == True:
			print("clean_snapshots:")
		snapshot_list=[]
		for snapshot in self.get_snapshots():
			snapshot_parts=snapshot.split("@")
			if snapshot_parts[1].startswith(prefix):
				snapshot_list.append(snapshot)

		number_to_remove= len(snapshot_list)-number_to_keep
		if number_to_remove >0:
			for snap_to_remove in snapshot_list[:number_to_remove]:
				self.destroy_snapshot(snap_to_remove=snap_to_remove)

	def clean_other_snapshots(self,prefixes_to_ignore=[], number_to_keep=None):
		if self.verbose == True:
			print("cloean_other_snapshots:")
		snapshot_list=[]
		for snapshot in self.get_snapshots():
			skip=False
			snapshot_parts=snapshot.split("@")
			for prefix in prefixes_to_ignore:
				if snapshot_parts[1].startswith(prefix):
					skip=True
					break
			if skip==False:
				snapshot_list.append(snapshot)

		number_to_remove= len(snapshot_list)-number_to_keep
		if number_to_remove >0:
			for snap_to_remove in snapshot_list[:number_to_remove]:
				self.destroy_snapshot(snap_to_remove=snap_to_remove)

	def destroy_snapshot(self,snap_to_remove):
		command=self.pool.remote_cmd+" zfs destroy "+self.verbose_switch()+snap_to_remove
		if self.verbose or self.dry_run:
			print(command)
		if not self.dry_run:
			try:
				subprocess.check_call(command, shell=True)
				self.pool.zfs_snapshots.remove(snap_to_remove)
			except subprocess.CalledProcessError as e:
				print(e)

	def timestamp_string(self):
		return datetime.datetime.today().strftime("%F--%H-%M-%S")

	def verbose_switch(self):
		if self.verbose==True:
			return "-v "
		else:
			return ""





def get_process_list(remote=""):
	ps = subprocess.Popen(remote+' ps aux', shell=True, universal_newlines=True,\
		stdout=subprocess.PIPE).communicate()[0]
	processes = ps.split('\n')
	nfields = len(processes[0].split()) - 1
	def proc_split(row):
		return row.split(None,nfields)
	return map(proc_split,processes[1:-1])

class TimeOut(Exception):
		def __init__(self):
				Exception.__init__(self,'Timeout ')

def _raise_TimeOut(sig, stack):
		raise TimeOut()

class TimeoutObject(object):
		def __init__(self, timeout, raise_exception=True):
				self.timeout = timeout
				self.raise_exception = raise_exception

		def __enter__(self):
				self.old_handler = signal.signal(signal.SIGALRM, _raise_TimeOut)
				signal.alarm(self.timeout)

		def __exit__(self, exc_type, exc_val, exc_tb):
				signal.signal(signal.SIGALRM, self.old_handler)
				signal.alarm(0)
				if exc_type is not TimeOut:
						return False
				return not self.raise_exception

def get_pids_for_cmd_line_parts(remote="",cmd_line_parts=[]):
	pids=[]
	for line in get_process_list(remote=remote):
		if len(line)<=1:
			continue
		for part in cmd_line_parts:
			if part not in line[-1]:
				break
		else:
			pids.append(line[1])
	return pids

def waitfor_cmd_to_exit(remote="", cmd_line_parts=[], sleep=5):
	pids=get_pids_for_cmd_line_parts(remote=remote,cmd_line_parts=cmd_line_parts)
	if len(pids)>0:
		while True:
			running=get_process_list(remote=remote)
			for line in running:
				if line[1] in pids:
					time.sleep(sleep)
					break # exit for loop
			else:
				break #no process found, exit while loop

