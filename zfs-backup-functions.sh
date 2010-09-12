zfs_backup_clean_snapshot() {
    TMPREMOTE=$2
    cmd="$TMPREMOTE zfs destroy  $1"

    if [ "x$3" != "x-y" ]; then
	echo $cmd" ? (Enter to confirm, non-empty line to reject)"
	answer=`line`
	if [ "x$answer" != "x" ]; then
	    return 0
	fi
    fi
    echo "Executing $cmd..."
    $cmd
    return 0
}

zfs_backup_clean_local(){
    for i in `zfs list  -t snapshot -o name |grep $SOURCE@|grep @$PREFIX|head -n -3`;  do
	zfs_backup_clean_snapshot "$i" "" "$1"
    done
    return 0
}

zfs_backup_clean_destination(){
    echo "cleaning destination: $REMOTE"
    for i in  `$REMOTE zfs list  -t snapshot -o name |grep $DEST@|grep @$PREFIX|head -n -3`; do
	zfs_backup_clean_snapshot  "$i" "$REMOTE" "$1"
    done
    return 0
}

zfs_backup_clean_destination_unrelated(){
    #remove backup- snapshots that don't correspond with the target pool but are there due to
    #incremental transferring
    for i in  `$REMOTE zfs list  -t snapshot -o name |grep $DEST@|grep @backup- |grep -v  @$PREFIX|head -n -3`; do
	zfs_backup_clean_snapshot "$i" "$REMOTE" "$1"
    done
    #others that we can forget about after some time
    for i in  `$REMOTE zfs list  -t snapshot -o name |grep $DEST@|grep @quarterly. |head -n -5`; do
	zfs_backup_clean_snapshot "$i" "$REMOTE" "$1"
    done
    for i in  `$REMOTE zfs list  -t snapshot -o name |grep $DEST@|grep @hourly. |head -n -5`; do
	zfs_backup_clean_snapshot "$i" "$REMOTE" "$1"
    done
    for i in  `$REMOTE zfs list  -t snapshot -o name |grep $DEST@|grep @daily. |head -n -7`; do
	zfs_backup_clean_snapshot "$i" "$REMOTE" "$1"
    done
    for i in  `$REMOTE zfs list  -t snapshot -o name |grep $DEST@|grep @weekly. |head -n -2`; do
	zfs_backup_clean_snapshot "$i" "$REMOTE" "$1"
    done
    for i in  `$REMOTE zfs list  -t snapshot -o name |grep $DEST@|grep @monthly. |head -n -2`; do
	zfs_backup_clean_snapshot "$i" "$REMOTE" "$1"
    done

    return 0
}

zfs_perform_backup(){

    start=`$REMOTE zfs list  -t snapshot -o name |grep $DEST@ |grep $PREFIX|awk -F@ '{print $2}'|tail -n 1`
    rollback_cmd="$REMOTE zfs rollback -r $DEST@$start"
    start=$SOURCE@$start

    stop=$SOURCE@$PREFIX-"`date "+%F--%H-%M-%S"`"
    snap_now_cmd="zfs snapshot $stop"

    transfer_cmd="zfs send -I $start $stop | $REMOTE zfs receive $DEST"

    echo "Commands to be run:"
    echo $rollback_cmd
    echo $snap_now_cmd
    echo $transfer_cmd

    if [ "x$1" != "x-y" ]; then
	echo $cmd" ? (Enter to confirm, non-empty line to reject)"
	answer=`line`
	if [ "x$answer" != "x" ]; then
	    return 1
	fi
    fi


#    echo "Do you want to continue? (y/N)"
#    answer=`line`
#    if [ "x$answer" == "xy" ]; then
	echo "running $rollback_cmd..."
	$rollback_cmd
	echo "running $snap_now_cmd..."
	$snap_now_cmd
	echo "running $transfer_cmd..."
	echo $transfer_cmd|bash

#    else
#        echo "No action taken."
#    fi
#    echo "Complete. Looking for older transfer-snapshots..."
	return 0
}



