# zfs-backup - script doBackup.py

Tool to perform zfs backup between two pools, potentially on different machines
doBackup Usage:

doBackup [options] -a backupAttribute -p backupPrefix -f fromPool -t toPool
doBackup [options] -a backupAttribute -p backupPrefix -j fromDataset -t toPool
doBackup [options] -i dataset -a backupAttribute
doBackup -l -a backupAttribute -f fromPool

The first form performs a backup of datasets with backupAttribute=yes in the fromPool to the toPool.

The second form specifies a dataset for single dataset backup under this program.  It sets the backup
attrbute if not already set and, if not present in the toPool and performs a single-dataset backup.

The third form initialises backupAttribute=yes in the specified dataset.  If a remote host is given,
it must be awake. The third form lists the datasets with the backup attribute set to 'yes'.

The backupPrefix parameter names the snapshots created,  e.g. "backup_".
If the same dataset is backed up to two different locations,  it can have the same backupAttribute,
but must have different backup prefixes so that the snapshot names do not interfere with each other.


Pools are [<hostname>[::<host mac address>:]]<poolname>
The  optional host specifies a pool on a remote machine.  In this case ssh root@hostname
must work without requiring a password, i.e. have id_rsa keys installed.   
The optional host mac address, if present
will cause the remote machine to be woken and put to sleep before and after the backup; and
assumes that the remote machine responds to wake-on-lan packets.

Datasets are [<hostname>::]<poolname>/<datasetname>,  which implies the root level
dataset cannot be backed up using this tool.

Other options:
-n  Notify email address in case of any failure - applies only to backup
-N  Dry run - just parse args
-v  verbose output
