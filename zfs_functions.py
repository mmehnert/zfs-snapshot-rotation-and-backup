'''
Created on 4 Sep 2012

@author: Maximilian Mehnert <maximilian.mehnert@gmx.de>
'''

import subprocess
import datetime
import signal
import time

class ZFS_iterator:
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
	remote_cmd=""
	pool=""
	zfs_filesystems=[]
	zfs_snapshots=[]

	def __init__(self,pool,remote_cmd=""):
		self.pool=pool
		self.remote_cmd=remote_cmd
		self.update_zfs_filesystems()
		self.update_zfs_snapshots()
	def update_zfs_snapshots(self, timeout=180):
		with TimeoutObject(timeout):
			waitfor_cmd_to_exit(remote=self.remote_cmd, cmd_line_parts=["zfs","list","snapshot"], sleep=5)
		snapshot_list=subprocess.check_output(self.remote_cmd+" zfs list -o name -t snapshot -H -r "+\
			self.pool, shell=True).split("\n")
		self.zfs_snapshots=snapshot_list
		return snapshot_list


	def update_zfs_filesystems(self):
		fs_list=subprocess.check_output(self.remote_cmd+' zfs list -o name -H -r '+self.pool,shell=True).split("\n")
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

	def get_zfs_filesystems(self,fs=""):
		toremove=[]
		fs_list=list(self.zfs_filesystems)
		for match_fs in fs_list:
			if not match_fs.startswith(fs):
				toremove.append(match_fs)
		map(fs_list.remove, toremove)
		return fs_list
	def __iter__(self):
		return ZFS_iterator(self)
	def get_origin(self,fs=None):
		origin=subprocess.check_output(self.remote_cmd+' zfs get origin '+fs,shell=True).split()
		origin=origin[6:7][0]
		return origin

	def get_zfs_filesystems(self, fs_filter=""):
		for fs in zfs_filesystems:
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

class ZFS_fs:
	fs=None
	pool=None

	def __init__(self,fs=None,remote_cmd="", pool=None):
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

def get_last_common_snapshot(src_fs=None, dst_fs=None):
	for dst_snapshot in dst_fs.get_snapshots_reversed():
		for src_snapshot in src_fs.get_snapshots_reversed():
			if dst_snapshot.split('@')[1] == src_snapshot.split('@')[1]:
				return src_snapshot
	return None

def get_last_snapshot(fs=None):
	ss=subprocess.check_output(
		fs.pool.remote_cmd+" zfs list -o name -t snapshot -H -r "+fs.fs+" |grep ^"+fs.fs+"@",shell=True).split("\n")[-2]
	return ss

def transfer_zfs_fs(src_fs=None, dst_fs=None, dry_run=False, verbose=False):
	if verbose:
		print("trying to transfer: "+src_fs.pool.remote_cmd+" "+src_fs.fs+" to "+dst_fs.pool.remote_cmd+" "+dst_fs.fs+".")

	if dst_fs.fs in dst_fs.pool.zfs_filesystems:
		print(dst_fs.fs+" already exists.")
		pass
	else:
		last_src_snapshot=get_last_snapshot(src_fs)
		if verbose:
			print("found "+last_src_snapshot)
		command=src_fs.pool.remote_cmd+" zfs send -R "+last_src_snapshot+" | "+dst_fs.pool.remote_cmd+" zfs receive "+dst_fs.fs
		if verbose or dry_run:
			print("running "+command)
		if not dry_run:
			subprocess.check_call(command,shell=True)
		return True

def sync_zfs_fs(src_fs=None,dst_fs=None,target_name="", dry_run=False, verbose=False):
	if verbose:
		print("Syncing "+src_fs.pool.remote_cmd+" "+src_fs.fs+" to "+dst_fs.pool.remote_cmd+" "+dst_fs.fs+" with target name "+target_name+".")

	if dst_fs.fs in dst_fs.pool.zfs_filesystems:
		last_common_snapshot=get_last_common_snapshot(src_fs=src_fs,dst_fs=dst_fs)
	else:
		if verbose:
			print(dst_fs.pool.remote_cmd+" "+dst_fs.fs+" does not exist.")
		last_common_snapshot=None

	if last_common_snapshot != None:
		sync_mark_snapshot=create_sync_mark_snapshot(fs=src_fs,target_name=target_name, dry_run=dry_run, verbose=verbose)
		if verbose:
			print("Sync mark created: "+src_fs.pool.remote_cmd+" "+sync_mark_snapshot)

		rollback=dst_fs.pool.remote_cmd+" zfs rollback -r "+dst_fs.fs+"@"+last_common_snapshot.split("@")[1]
		sync_command=src_fs.pool.remote_cmd+" zfs send -I "+last_common_snapshot+" "+sync_mark_snapshot+ "|"+dst_fs.pool.remote_cmd+" zfs receive "+dst_fs.fs

		if dry_run==True:
			print(rollback)
			print(sync_command)
			return True
		else:
			if verbose:
				print("Running rollback: "+rollback)
			subprocess.check_call(rollback,shell=True)

			if verbose:
				print("Running sync: "+sync_command)
			subprocess.check_call(sync_command,shell=True)

			dst_fs.pool.update_zfs_snapshots()
			sync_mark=sync_mark_snapshot.split("@")[1]
			for snap in dst_fs.get_snapshots():
				if snap.split("@")[1]==sync_mark:
					if verbose:
						print("Sucessfully transferred "+sync_mark_snapshot)
					return True
	else:
			create_sync_mark_snapshot(fs=src_fs,target_name=target_name, dry_run=dry_run, verbose=verbose)
			return transfer_zfs_fs(src_fs=src_fs, dst_fs=dst_fs, dry_run=dry_run, verbose=verbose)
	return False

def timestamp_string():
	return datetime.datetime.today().strftime("%F--%H-%M-%S")

def create_sync_mark_snapshot(fs=None, target_name="",dry_run=False, verbose=False):
	if len(target_name) == 0:
		raise ValueError("target_name for synchronization markers must be defined")
	sync_mark_snapshot=fs.fs+"@"+target_name+"-"+timestamp_string()
	command=fs.pool.remote_cmd+" zfs snapshot "+sync_mark_snapshot
	if dry_run == True:
		print(command)
	else:
		if verbose:
			print("Running "+command)
		subprocess.check_call(command,shell=True)
	return sync_mark_snapshot


def is_zfs_scrub_running(pool=None):
	zfs_output=subprocess.check_output(pool.remote_cmd+" zpool status "+pool.pool, shell=True)
	return  "scrub in progress" in zfs_output

def create_zfs_snapshot(fs=None,prefix="", dry_run=False, verbose=False):
	if len(prefix)==0:
		raise ValueError("prefix for snapshot must be defined")
	snapshot_command=fs.pool.remote_cmd+" zfs snapshot "+fs.fs+"@"+prefix+"-"+timestamp_string()
	if verbose or dry_run:
		print(snapshot_command)
	if not dry_run:
		subprocess.check_call(snapshot_command, shell=True)

def clean_zfs_snapshots(fs=None, prefix="", number_to_keep=None, dry_run=False, verbose=False):
	snapshot_list=[]
	for snap in fs.get_snapshots():
		snapshot_list.append(snap)
	toremove=[]
	for snapshot in snapshot_list:
		snapshot_parts=snapshot.split("@")
		if (not snapshot_parts[1].startswith(prefix)) or (snapshot_parts[0]!=fs.fs):
			toremove.append(snapshot)
	map(snapshot_list.remove, toremove)

	number_to_remove= len(snapshot_list)-number_to_keep
	if number_to_remove >0:
		for snap_to_remove in snapshot_list[:number_to_remove]:
			command=fs.pool.remote_cmd+" zfs destroy "+verbose_switch(verbose)+snap_to_remove
			if verbose or dry_run:
				print(command)
			if not dry_run:
				try:
					subprocess.check_call(command, shell=True)
				except subprocess.CalledProcessError as e:
					print(e)

def get_process_list(remote=""):
	ps = subprocess.Popen(remote+' ps aux', shell=True, stdout=subprocess.PIPE).communicate()[0]
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



def verbose_switch(verbose=False):
	if verbose==True:
		return "-v "
	else:
		return ""

def clean_other_zfs_snapshots(fs=None, prefixes_to_ignore=[], number_to_keep=None, dry_run=False, verbose=False):
	snapshot_list=[]
	for snap in fs.get_snapshots():
		snapshot_list.append(snap)
	toremove=[]
	for snapshot in snapshot_list:
		snapshot_parts=snapshot.split("@")
		for prefix in prefixes_to_ignore:
			if snapshot_parts[1].startswith(prefix):
				toremove.append(snapshot)
				break
		else:
			if snapshot_parts[0]!=fs.fs:
				toremove.append(snapshot)
	map(snapshot_list.remove, toremove)

	number_to_remove= len(snapshot_list)-number_to_keep
	if number_to_remove >0:
		for snap_to_remove in snapshot_list[:number_to_remove]:
			command=fs.pool.remote_cmd+" zfs destroy "+verbose_switch(verbose)+snap_to_remove
			if verbose or dry_run:
				print(command)
			if not dry_run:
				try:
					subprocess.check_call(command, shell=True)
				except subprocess.CalledProcessError as e:
					print(e)


