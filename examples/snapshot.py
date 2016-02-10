#!/usr/bin/python

'''
Created on 6 Sep 2012

@author: Maximilian Mehnert <maximilian.mehnert@gmx.de>
'''

import argparse
import sys

from zfs_functions import *

if __name__ == '__main__':
	parser=argparse.ArgumentParser(description="take some snapshots")
	parser.add_argument("fs", help="The zfs filesystem to act upon")
	parser.add_argument("prefix",help="The prefix used to name the snapshot(s)")
	parser.add_argument("-r",help="Recursively take snapshots",action="store_true")
	parser.add_argument("-k",type=int, metavar="n",help="Keep n older snapshots with same prefix. Otherwise delete none.")
	parser.add_argument("--dry-run", help="Just display what would be done. Notice that since no snapshots will be created, less will be marked for theoretical destruction. ", action="store_true")
	parser.add_argument("--verbose", help="Display what is being done", action="store_true")
	parser.add_argument("--remote", help="e.g. \"ssh hostname\"", default="")

	args=parser.parse_args()
	print(args)

	try:
		pool=ZFS_pool(pool=args.fs.split("/")[0],remote_cmd=args.remote)
	except subprocess.CalledProcessError:
		sys.exit()
	fs_obj=ZFS_fs(fs=args.fs, pool=pool)
	if args.r==False:
		create_zfs_snapshot(fs=fs_obj,prefix=args.prefix,dry_run=args.dry_run, verbose=args.verbose)
		if args.k != None and args.k >= 0:
			#if we are here, the snapshot was created. We did not update, so subtract 1 from args.k
			clean_zfs_snapshots(fs=fs_obj, prefix=args.prefix,
				number_to_keep=args.k-1,dry_run=args.dry_run,
				verbose=args.verbose)

	else:
		for fs in pool.get_zfs_filesystems(fs=fs_obj.fs):
			fs=ZFS_fs(fs=fs,pool=pool)
			create_zfs_snapshot(fs=fs,
				prefix=args.prefix,dry_run=args.dry_run,
				verbose=args.verbose)
			if args.k != None and args.k >= 0:
				clean_zfs_snapshots(fs=fs, prefix=args.prefix,
					number_to_keep=args.k-1,dry_run=args.dry_run,
					verbose=args.verbose)

