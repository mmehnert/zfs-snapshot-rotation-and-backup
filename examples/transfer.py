#!/usr/bin/python
'''
@author: Maximilian Mehnert <maximilian.mehnert@gmx.de
'''

from	zfs_functions import *



if __name__ == '__main__':
	transfer_marker_prefix="backup"
	# we are transferring / synchronizing the pool all from 192.168.50.20 to the local machine
	src_pool=ZFS_pool(pool="all", remote_cmd="ssh 192.168.50.20", verbose=True)
	dst_pool=ZFS_pool(pool="all", verbose=True)
	# the copy of the remote pool "all" will end up in "all/backup/all" on the local machine
	dst_prefix="all/backup/"

	for fs in src_pool:
		print (fs)
		src_fs=ZFS_fs(fs=fs, pool=src_pool, verbose=True)
		dst_fs=ZFS_fs(fs=dst_prefix+fs,pool=dst_pool, verbose=True)

		if not src_fs.sync_with(dst_fs=dst_fs,target_name=transfer_marker_prefix):
			print ("sync failure for "+fs)
		else:
			# here we are deleting older snapshots starting with transfer_marker_prefix, in this case keeping
			# the youngest three
			src_fs.clean_snapshots(prefix=transfer_marker_prefix, number_to_keep=3)
			# on the destination pool (in this case local machine, see above), clean up snapshots starting with the prefixes below,
			# keeping the youngest n according to the second part of the tuples
			for tuple in [["5min",1],
										["hourly",1],
										["quarterly",1],
										["daily",7],
										["weekly",4],
										["monthly",12]]:
				dst_fs.clean_snapshots(prefix=tuple[0], number_to_keep=tuple[1])
			# now we clean up unrelated snapshots both on the remote and local machine, ignoring those starting with the prefixes
			# configured below, keeping the 10 youngest
			prefixes_to_ignore=["hourly","weekly","quarterly","daily","weekly","monthly","yearly",transfer_marker_prefix]
			dst_fs.clean_other_snapshots(prefixes_to_ignore=prefixes_to_ignore,number_to_keep=10)
			src_fs.clean_other_snapshots(prefixes_to_ignore=prefixes_to_ignore,number_to_keep=10)

