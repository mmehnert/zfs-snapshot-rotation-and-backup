'''
Created on 4 Sep 2012

@author: Maximilian Mehnert <maximilian.mehnert@gmx.de>
'''

import subprocess
import datetime

class ZfsException(Exception):
  def __init__(self, value):
    self.parameter = value
  def __str__(self):
    return repr(self.parameter)

def get_zfs_filesystems(remote="", fs=""):
  fs=subprocess.check_output(remote+' zfs list -o name -H -r '+fs,shell=True).split("\n")
  fs=fs[0:-1]
  return fs

def get_zfs_snapshots(remote="", fs=""):
  ss=subprocess.check_output(remote+" zfs list -o name -t snapshot -H -r "+fs+" |grep "+fs+"@",shell=True).split("\n")
  ss=ss[0:-1]
  
  return ss

def get_last_common_snapshot(remote_src="", remote_dst="", fs_src="", fs_dst=""):
  ss_src=get_zfs_snapshots(remote=remote_src,fs=fs_src)
  ss_dst=get_zfs_snapshots(remote=remote_dst,fs=fs_dst)
  for dst_snapshot in reversed(ss_dst):
    for src_snapshot in reversed(ss_src):
      if dst_snapshot.split('@')[1] == src_snapshot.split('@')[1]:
        return src_snapshot  
  return None

def get_last_snapshot(remote="", fs=""):
  ss=subprocess.check_output(
    remote+" zfs list -o name -t snapshot -H -r "+fs+" |grep "+fs+"@",shell=True).split("\n")[-2]
  return ss

def transfer_zfs_fs(remote_src="", remote_dst="", fs_src="", fs_dst="", dry_run=False, verbose=False):
  if verbose:
    print "trying to transfer: "+remote_src+" "+fs_src+" to "+remote_dst+" "+fs_dst+"."
  dst_pool=fs_dst.split("/")[0]  
  target_fs_list=get_zfs_filesystems(remote=remote_dst, fs=dst_pool)
  
  if fs_dst in target_fs_list:
    #target exists
    pass
  else:
    #transfer
    last_remote_snapshot=get_last_snapshot(remote=remote_src,fs=fs_src)
    if verbose:
      print "found "+last_remote_snapshot
    command=remote_src+" zfs send -R "+last_remote_snapshot+" | "+remote_dst+" zfs receive "+fs_dst
    if verbose or dry_run:
      print "running "+command
    if not dry_run: 
      subprocess.check_call(command,shell=True)
    return True
  
def sync_zfs_fs(remote_src="", remote_dst="", fs_src="", fs_dst="",target_name="", dry_run=False, verbose=False):
  if verbose:
    print "Syncing "+remote_src+" "+fs_src+" to "+remote_dst+" "+fs_dst+" with target name "+target_name+"."

  dst_pool=fs_dst.split("/")[0]
  target_fs_list=get_zfs_filesystems(remote=remote_dst, fs=dst_pool)
  if fs_dst in target_fs_list:
    last_common_snapshot=get_last_common_snapshot(remote_src=remote_src,remote_dst=remote_dst,fs_src=fs_src,fs_dst=fs_dst)
  else:
    if verbose:
      print remote_dst+" "+fs_dst+" does not exist."
    last_common_snapshot=None
        
  if last_common_snapshot != None:
    #last_src_snapshot=get_last_snapshot(remote=remote_src,fs=fs_src)
    sync_mark_snapshot=create_sync_mark_snapshot(remote_src=remote_src,fs_src=fs_src,target_name=target_name, dry_run=dry_run, verbose=verbose)
    if verbose:
      print "Sync mark created: "+remote_src+" "+sync_mark_snapshot
      
    rollback=remote_dst+" zfs rollback -r "+fs_dst+"@"+last_common_snapshot.split("@")[1]
    sync_command=remote_src+" zfs send -I "+last_common_snapshot+" "+sync_mark_snapshot+ "|"+remote_dst+" zfs receive "+fs_dst

    if dry_run==True:
      print rollback
      print sync_command
    else:
      if verbose:
        print "Running rollback: "+rollback
      subprocess.check_call(rollback,shell=True)
      
      if verbose:
        print "Running sync: "+sync_command  
      subprocess.check_call(sync_command,shell=True)
      
      local_snapshots=get_zfs_snapshots(fs=fs_dst)
      sync_mark=sync_mark_snapshot.split("@")[1]
      for snap in local_snapshots:
        if snap.split("@")[1]==sync_mark:
          if verbose:
            print "Sucessfully transferred "+sync_mark_snapshot
          return True
  else:
      return transfer_zfs_fs(remote_src=remote_src, remote_dst=remote_dst, fs_src=fs_src, fs_dst=fs_dst, dry_run=dry_run, verbose=verbose)
  return False

def timestamp_string():
  return datetime.datetime.today().strftime("%F--%H-%M-%S")

def create_sync_mark_snapshot(remote_src="", fs_src="", target_name="",dry_run=False, verbose=False):
  if len(target_name) == 0: 
    raise ValueError, "target_name for synchronization markers must be defined"
  sync_mark_snapshot=fs_src+"@"+target_name+"-"+timestamp_string()
  command=remote_src+" zfs snapshot "+sync_mark_snapshot
  if dry_run == True:
    print command
  else:
    if verbose:
      print "Running "+command
    subprocess.check_call(command,shell=True)
  return sync_mark_snapshot
  
   
def is_scrub_running(fs=""):
  pool=fs.split("/")[0]
  zfs_output=subprocess.check_output("zfs status "+pool)
  return "scrub in process" in zfs_output
  
def create_snapshot(fs="",prefix=""):
  pass
