#!/usr/bin/python
'''
Created on 3 Sep 2012

@author: Maximilian Mehnert <maximilian.mehnert@gmx.de
'''

from  zfs_functions import *



if __name__ == '__main__':
  transfer_marker_prefix="backup"
  dry_run=False
  # we are transferring / synchronizing the pool all from 192.168.50.20 to the local machine
  src_pool=ZFS_pool(pool="all", remote_cmd="ssh 192.168.50.20")
  dst_pool=ZFS_pool(pool="all")
  # the copy of the remote pool "all" will end up in "all/backup/all" on the local machine
  dst_prefix="all/backup/"
  
  for fs in src_pool.zfs_filesystems: 
    print fs
    src_fs=ZFS_fs(fs=fs, pool=src_pool)    
    dst_fs=ZFS_fs(fs=dst_prefix+fs,pool=dst_pool)

    if not sync_zfs_fs(src_fs=src_fs, dst_fs=dst_fs,target_name=transfer_marker_prefix,
                       verbose=True, dry_run=dry_run):
      print "sync failure for "+fs
    else:
      # here we are deleting older snapshots starting with transfer_marker_prefix, in this case keeping
      # the youngest three
      clean_zfs_snapshots(src_fs, prefix=transfer_marker_prefix,
                           number_to_keep=3, dry_run=dry_run, verbose=True)
      clean_zfs_snapshots(dst_fs, prefix=transfer_marker_prefix,
                           number_to_keep=3, dry_run=dry_run, verbose=True)
      # on the destination pool (in this case local machine, see above), clean up snapshots starting with the prefixes below,
      # keeping the youngest n according to the second part of the tuples
      for tuple in [["5min",1],
                    ["hourly",1],
                    ["quarterly",1],
                    ["daily",7],
                    ["weekly",4],
                    ["monthly",12]]:
        clean_zfs_snapshots(fs=dst_fs, prefix=tuple[0],
                      number_to_keep=tuple[1],dry_run=dry_run, verbose=True)      
      # now we clean up unrelated snapshots both on the remote and local machine, ignoring those starting with the prefixes
      # configured below, keeping the 10 youngest
      prefixes_to_ignore=["hourly","weekly","quarterly","daily","weekly","monthly","yearly",transfer_marker_prefix]
      clean_other_zfs_snapshots(fs=dst_fs, prefixes_to_ignore=prefixes_to_ignore,number_to_keep=10, dry_run=dry_run, verbose=verbose)
      clean_other_zfs_snapshots(fs=src_fs, prefixes_to_ignore=prefixes_to_ignore,number_to_keep=10, dry_run=dry_run, verbose=verbose)

