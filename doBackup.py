# A dataset is marked for backup using the zfs "backup" attribute
# An invokation of the program indicates "local" or "both" in its command-line argument.  A cron job
# calls this program with the appropriate parameter and different periodicities.

import sys
import subprocess
import os
import time
from timeit import default_timer as timer
import requests



class Log:
    # Severity enumeration
    VERBOSE=0
    INFO=1
    PROGRESS=2
    WARN=3
    ERROR=4
    def __init__(self,print_severity=INFO):
        # this is a list of tuples:  (message, severity)
        self.clear()
        self.print_severity = print_severity # print messages at or above
        self.disable_count = 0

    def setPrintSeverity(self, print_severity):
        self.print_severity = print_severity
    
    def clear(self):
        self.messages=[]

    # These two routines allow benign errors to be masked out of the log
    def disable(self):
        self.disable_count += 1

    def enable(self):
        if self.disable_count > 0:
            self.disable_count -= 1

    def add(self,text,severity=INFO):
        if self.disable_count == 0:

            # remove any trailing newlines
            while (len(text) > 0) and (text[-1] == '\n'):
                text=text[:-1]

            message=(text,severity)
            self.messages.append(message)
            if severity >= self.print_severity:
                print (text)



    def isEmpty(self):
        return len(self.messages) == 0

    def getFilteredMessages(self, wanted_severity=INFO):
        s = ''
        lines = [text for (text,severity) in self.messages if severity >= wanted_severity]
        for line in lines:
            s += line + '\n'
        return s

    def __repr__(self):
        return self.getFilteredMessages()



# This class represents a source or destination of a backup
# Uses the openssh client Master feature.
# Execution of test_backup tests with host=nas3 takes 16s with useMaster and 190s without useMaster
class Pool:
    def __init__(self,name,hostname=None,hostmac=None,backupPrefix='backup_',backupAttribute='cs:backup',maxBackupSnaps=5,useMaster=True,log=None):
        self.name=name
        self.hostname=hostname
        self.hostmac=hostmac
        self.backupPrefix=backupPrefix  # prefix to snapshot name, only significant in source pool
        self.backupAttribute = backupAttribute # zfs attribute flagging dataset as master - only significant in source pool
        self.maxBackupSnaps=maxBackupSnaps
        self.ssh=''
        self.useMaster = useMaster
        self.log = log if log else Log()
        self.exceptionsOn=True
        self.hostString=''
        self.verboseInfo=False

        if self.hostname:
            self.hostString=f'{self.hostname}:'
            if self.useMaster:
                self.masterPopen=None
                self.masterSocket = f'./master-socket-{self.hostname}'
                self.sshParams= f'-S {self.masterSocket}'
                self.ssh = f'ssh root@{self.hostname} {self.sshParams}'
            else:
                self.ssh = f'ssh root@{self.hostname} '

    def setExceptions(self,e):
        self.exceptionsOn=e

    def wake(self):
        if self.hostname:
            if not self.hostmac:
                raise Exception(f'Cannot wake: {self.hostname}, MAC address not configured')
            subprocess.run(['/bin/bash', '-c', f'etherwake {self.hostmac}'], capture_output=True)
            time.sleep(2)       # Give a little time for destination to resume
        
    def sleep(self): # I never did get this to run reliably.  Instead I call a shell script
        # to wake/sleep the target around call to this script.
        try:
#            self.run('(systemctl suspend) <&- >&- 2>&- &')
            self.run('systemctl restart systemd-suspend.service', timeout=5)
        except: # It's either asleep or the suspend service hangs ssh
            pass


    def checkMaster(self): # check for existence of ssh master
        if self.hostname and self.useMaster:
            result=subprocess.run(['/bin/bash', '-c', f'{self.ssh} -O check'], capture_output=True)
            if result.returncode != 0:
                if os.path.exists(self.masterSocket):
                    os.remove(self.masterSocket)
                self.masterPopen=subprocess.Popen(['/bin/bash', '-c', f'{self.ssh} -MN'])
                for _i in range(5):
                    # allow up to 10 seconds for the master open to succeed
                    if os.path.exists(self.masterSocket):
                        return
                    time.sleep(1)
                # Kill master and raise exception
                self.masterPopen.kill()
                raise Exception(f'ssh cannot talk to {self.hostname}')

    def __repr__(self):
        return f'{self.hostString}{self.name}'

    # Destructor,  release master process if we own it
    def __del__(self):
        if self.hostname and self.useMaster and self.masterPopen:
            self.masterPopen.kill()
            self.masterPopen = None


    def run(self,command,timeout=120):
        self.checkMaster()
        if self.hostname:
            command=f'"{command}"'
        result=subprocess.run(['/bin/bash', '-c', f'{self.ssh} {command}'], capture_output=True, timeout=timeout)
        return self.processResult(result)

    # Process the result (stderr, stdout) of a subprocess run
    def processResult(self,result):
        s = ''
        if result.returncode == 0:
            s = result.stdout.decode('utf-8')
            if len(s) > 0:
                self.log.add(s, self.log.VERBOSE)
        else:
            e = result.stderr.decode('utf-8')
            self.log.add(e, self.log.ERROR)
            if self.exceptionsOn:
                raise Exception(e)
        return s

    # Returns a list of output lines from run
    def runList(self,command):
        output=self.run(command)
        if len(output) == 0:
            return []   # empty list
        else:
            return [line for line in output.split('\n') if len(line) > 0]


    def run2(self,command,pool2,command2):
        # Run first command in context of first pool and pipe
        # output to second command in context of second pool
        if self.hostname == pool2.hostname:
            # On same host
            c=f'{command} | {command2}'
            if self.hostname:
                c=f'{self.ssh} "{c}"'
        else:
            # on different hosts
            # if we are going to use ssh,  quote command
            if self.hostname:
                command=f'"{command}"'
            if pool2.hostname:
                command2=f'"{command2}"'                
            c=f'{self.ssh} {command} | {pool2.ssh} {command2}'

        result=subprocess.run(['/bin/bash', '-c', c], capture_output=True)
        return self.processResult(result)


    def dataset(self,ds_name):
        return Dataset(self,ds_name)

    def getBackupDatasets(self):
        lines=self.runList(f'zfs get -r -o name,value -s local -H {self.backupAttribute} {self.name}')
        datasets=[]
        for line in lines:
            if len(line) > 0:
                words=line.split('\t')
                fullds=words[0]
                ds_parts=fullds.split('/', 1)
#                ds_pool=ds_parts[0]
                ds_name=ds_parts[1]
                attribute_value=words[1]
                if attribute_value == "yes":
                    datasets.append(ds_name)
        return datasets

    def poolBackup(self,tPool):
        # Backup self to destination pool
        try:
            datasets=self.getBackupDatasets()
        except:
            self.log.add(f'{self} cannot get backup datasets ',self.log.ERROR)
            return 0,1

        self.log.add(f'Entry to Backup {self} to {tPool} has {len(datasets)} datasets to process')
        nSucceeds=0
        nFails=0
        for ds_name in datasets:
            ds=self.dataset(ds_name)
            tds=tPool.dataset(ds_name)
            startTime = timer()
            status='unknown'
            try:
                try:
                    backupAttribute = tds.attrGet(self.backupAttribute)
                except:     # Generates exception if tds does not exist
                    backupAttribute = None
                if backupAttribute == 'yes':
                    s = f'destination {tds} is marked as a backup source {self.backupAttribute}=yes'
                    self.log.add(s,self.log.ERROR)
                    raise Exception(s)
                if ds.dsBackup(tPool):
                    status = 'succeeds'
                else:
                    status = 'unchanged'
                nSucceeds += 1
            
            except:
                status='fails'
                nFails += 1
                pass
            endTime = timer()
            duration = endTime-startTime
            if status=='fails':
                self.log.add(f'Error: {ds} {status} after {duration:.1f} s.',self.log.ERROR)
            else:
                self.log.add(f'{ds} {status} after {duration:.1f} s.',self.log.PROGRESS)
                
        return nSucceeds,nFails


# Non-snapshot dataset
class Dataset:
    def __init__(self,pool,ds_name):
        self.pool = pool
        self.ds_name = ds_name
        self.d=f'/{self.ds_name}' if self.ds_name else ''    

    def __repr__(self):
        return f'{self.pool}{self.d}'

    # Create dataset
    def dsCreate(self):
        self.pool.log.add(f'dsCreate {self}')
        self.pool.run(f'zfs create {self.pool.name}/{self.ds_name}')

    # Roll back dataset to specified snap
    def dsRollback(self,snap):
        self.pool.log.add(f'dsRollback {self}@{snap}')
        d='/'+self.ds_name if self.ds_name else ''
        self.pool.run(f'zfs rollback -r {self.pool.name}{d}@{snap}')

    # Destroy dataset - fails if any snapshots
    def dsDestroy(self,recursive=False):
        self.pool.log.add(f'dsDestroy {self} ({"recursive" if recursive else "non-recursive"})')        
        self.pool.run(f'zfs destroy {"-R" if recursive else ""} {self.pool.name}{self.d}')

    # Determine if dataset exists
    def dsExists(self):
        # Return true iff the dataset exists
        try:
            self.pool.log.disable()
            self.pool.run(f'zfs list -H -o name {self.pool.name}/{self.ds_name}')
            self.pool.log.enable()
            return True
        except: # Exception if it does not exist
            return False

    # Returns true if the dataset has changed since the specified snap
    # Uses the "written" property of a snapshot,  which is the amount written
    # between the previous and that snapshot.   So,  from the specified snapshot
    # we look at the written values for all subsequent snapshots to know if anything
    # changed since the specified snapshot.
    # Relies on there being a recent last snapshot.
    def dsGetChangedSince(self, snap_name):

        # Get the list of snaps for this ds
        snap_lines=self.pool.runList(f'zfs list -rt snapshot -H -o name,written {self.pool.name}{self.d}') 
        found=False     # Indicates if we found the specified snap_name yet
        for snap_line in snap_lines:
            if '\t' in snap_line:
                # Parse the snap line into snapshot name and changed
                words=snap_line.split('\t')
                this_full_ds_name=words[0]
                assert '@' in this_full_ds_name

                # Extract the snapshot name
                this_snap_name = this_full_ds_name.split('@')[1]
                
                # If matches wanted snap,  start paying attention to the used value
                if this_snap_name == snap_name:
                    found = True
                else:
                    this_used_text=words[1]
                    this_changed = False if this_used_text == '0' else True

                    if found and this_changed:
                        return True
        return False

    # Backup a single dataset from own pool to another
    # return True if backup performed or False if no changes made
    def dsBackup(self, tPool):
        self.pool.log.add(f'dsBackup {self} to {tPool}')        
        INITIAL_SNAP_SUFFIX='initial'
        snap_initial = self.pool.backupPrefix + INITIAL_SNAP_SUFFIX

        # Destination dataset
        tds=tPool.dataset(self.ds_name)

        def backupNew():
            # Create new backup
            # Delete any old backup snapshots
            self.pool.log.add(f'dsBackup {self} is initial backup')                
            self.snapsDelete(self.pool.backupPrefix)

            # Create initial backup
            self.snapCreate(snap_initial)

            self.pool.run2(f'zfs send -p {self.pool.name}{self.d}@{snap_initial}',tPool,f'zfs receive -u {tPool.name}{tds.d}')
            tPool.run(f'zfs set com.sun:auto-snapshot=false {tPool.name}{tds.d}')
            tPool.run(f'zfs inherit {tPool.backupAttribute} {tPool.name}{tds.d}')

        # destination does not exist, create it
        if not tds.dsExists():
            # Create new
            backupNew()
        else:

            # destination exists, get most recent common snap
            mr_snap = self.snapsGetLatestCommon(tds)
            self.pool.log.add(f'dsBackup {self} to {tPool} lastest common snap is {mr_snap}')                

            # if no common snap, delete and re-create destination
            if not mr_snap:
                self.pool.log.add(f'dsBackup {self} to {tPool} no common snaps')                    
                tds.dsDestroy(recursive=True)
                backupNew()
            else:
                # perform incremental backup
                # temporary ensure no snapshots locally at the destination
                # tPool.run(f'zfs set com.sun:auto-snapshot=false {tPool.name}{tds.d}')

                # get latest backup snap name
                snaps=self.snapsGet(filter=self.pool.backupPrefix)
                if len(snaps) == 0:
                    # destination not created by this program.  That's OK.
                    snap_index=1
                else:
                    previous_snap=snaps[-1]
                    parts=previous_snap.split(self.pool.backupPrefix)
                    assert len(parts)==2
                    suffix=parts[1]
                    if suffix==INITIAL_SNAP_SUFFIX:
                        snap_index=1
                    else:
                        try:
                            previous_number=int(parts[1])
                        except:
                            previous_number=0       # Name collision with non-us backup_ prefix
                        snap_index=previous_number + 1

                snap_name = f'{self.pool.backupPrefix}{snap_index:06}'
                # Snapshot the from ds
                self.snapCreate(snap_name)                

                # Determine if anything needs to be done.  Yes if anything changed since
                # the most recent common snap.
                # If the most recent common snap is a zrep, we will force the backup even if
                # no changes because the zrep snapshot might go away,  as it is not under
                # our control

                if self.dsGetChangedSince(mr_snap) or ('zrep' in mr_snap):



                    # Roll back the destination to the common snap
                    tds.dsRollback(mr_snap)

                    # perform an incremental backup
                    self.pool.log.add(f'dsBackup {self} to {tPool} incremental backup {mr_snap} to {snap_name}')
                    try:                    
                        self.pool.run2(f'zfs send -I {mr_snap} {self.pool.name}{self.d}@{snap_name}',tPool,f'zfs receive {tPool.name}{tds.d}')

                    except Exception as e:
                        # backup fails - prevent growth of unmatched backup snaps
                        self.snapDelete(snap_name)

                        # backup based on mr_snap failed,  so delete it
                        tds.snapDelete(mr_snap)
                        raise e                        

                    # trim backup snaps in source and destination
                    self.snapsTrim(self.pool.maxBackupSnaps,filter=self.pool.backupPrefix)
                    tds.snapsTrim(tds.pool.maxBackupSnaps,filter=self.pool.backupPrefix)
                    
                    # trim snaps not in the  from pool
                    self.snapsTrimToSource(tds)

                else:
                    # No changes
                    info = f'No changes since {self}@{mr_snap} - no action taken'
                    self.pool.log.add(info,self.pool.log.INFO)

                    # Remove the unnecessary snapshot
                    self.snapDelete(snap_name)
                    return False    # No change,  no backup performed
        return True


    # Attribute methods.  These only work on the dataset,  not the snapshot level
    # Determine if specified attribute exists
    def attrExists(self,attr_name,snap=None):
        s='@'+snap if snap else ''
        try:
            lines=self.pool.runList(f'zfs get {attr_name} -o value -s local -H {self.pool.name}{self.d}{s}')
            return (len(lines) == 1) and (lines[0] != '-')
        except: # no such pool or dataset
            return False

    # Get value of a zfs dataset attribute, or None
    def attrGet(self,attr_name,snap=None):
        s='@'+snap if snap else ''           
        lines=self.pool.runList(f'zfs get {attr_name} -o value -s local -H {self.pool.name}{self.d}{s}')
        if (len(lines) == 1) and (lines[0] != '-'):
            return lines[0]
        else:
            return None

    # Set value of an attribute
    def attrSet(self,attr_name,value,snap=None):
        self.pool.log.add(f'attrSet {self} {attr_name} to {value}')
        s='@'+snap if snap else ''
        self.pool.run(f'zfs set {attr_name}={value} {self.pool.name}{self.d}{s}')       


    # Delete attribute
    def attrDelete(self,attr_name,snap=None):
        self.pool.log.add(f'attrDelete {self} {attr_name}')
        s='@'+snap if snap else ''           
        self.pool.run(f'zfs inherit {attr_name} {self.pool.name}{self.d}{s}')    


    # Snapshot methods
    def snapExists(self,snap):
        # Return true iff the dataset exists
        try:
            self.pool.run(f'zfs list -H -o name {self.pool.name}{self.d}@{snap}')
            return True
        except: # Exception if it does not exist
            return False

    # Get list of snapshots
    def snapsGet(self,filter=None):
        command=f'zfs list -H -o name -d 1 -t snapshot {self.pool.name}{self.d}'
        try:
            fullNames=self.pool.runList(command)
            # Parse keeping snapshot name after @
            snaps = [fullName.split('@')[1] for fullName in fullNames]
            return doFilter(snaps,filter)
        except: 
            return []

    # Create a snapshot
    def snapCreate(self,snap):
        self.pool.log.add(f'snapCreate {self}@{snap}')        
        self.pool.run(f'zfs snapshot {self.pool.name}{self.d}@{snap}')

    # Delete a snapshot
    def snapDelete(self,snap):
        self.pool.log.add(f'snapDelete {self}@{snap}')
        try:        
            self.pool.run(f'zfs destroy {self.pool.name}{self.d}@{snap}')
        except:
            # allow one failure, which might be because zfs autosnapshot is taking place
            # delay and try again
            time.sleep(300)
            self.pool.log.add(f'snapDelete retry {self}@{snap}')            
            self.pool.run(f'zfs destroy {self.pool.name}{self.d}@{snap}')
            self.pool.log.add(f'snapDelete retry {self}@{snap} succeeds')



    # Delete matching snapshots
    def snapsDelete(self,filter=None):
        snaps=self.snapsGet(filter)
        for snap in snaps:
            self.snapDelete(snap)

    # Trim snapshots to the last n
    def snapsTrim(self,n,filter=None):
        snaps=self.snapsGet(filter)
        if len(snaps) > n:
            # list slice for first n snaps     
            for snap in snaps[0:len(snaps)-n]:
                self.snapDelete(snap)

    # Trim snapshots in destination (tds) not present in self
    def snapsTrimToSource(self,tds):      
        to_snaps=tds.snapsGet()

        # create a set from current snaps in destination
        current_set=set()
        for s in to_snaps:
            current_set.add(s)

        # create an allowable set from source pool
        from_snaps=self.snapsGet()
        allow_set=set()
        for s in from_snaps:
            allow_set.add(s)

        # Delete snaps not present in allow set
        for s in current_set:
            if s not in allow_set:
                tds.snapDelete(s)



    # Get the most recent snap that is present in pools p1 and p2
    # return None if no matching snap
    def snapsGetLatestCommon(self,d2):

        snaps1=self.snapsGet()
        snaps2=d2.snapsGet()

        # Add all snaps2 into a set
        set2=set()
        for snap2 in snaps2:
            set2.add(snap2)

        # for snaps in snaps1, in reverse order, check
        # if exists in set2 and exit if so
        for snap1 in snaps1[::-1]:
            if snap1 in set2:
                return snap1
        return None


# return subset of list items that match filter
def doFilter(items, filter):
    if filter == None:
        return items
    else:
        return [item for item in items if filter in item]


from mymail import mail




usageText="""
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
"""

def usage():
    print (usageText)

def main(argv):
    import getopt
    log=Log()
    nFails = 0      # number of failures

    def parse_poolordataset(s):
        # Parse dataset or pool specification
        hostname=None
        hostmac=None
        poolname=s
        ds_name=None
        if '::' in s:
            words=s.split('::')
            if len(words) == 3: # mac present
                hostname=words[0]
                hostmac=words[1]
                poolname=words[2]
            else:
                hostname=words[0]
                poolname=words[1]

        if '/' in poolname:     # dataset name present
            # Mac address present
            words=poolname.split('/',1)
            poolname=words[0]
            ds_name=words[1]
                
        return hostname, hostmac, poolname, ds_name


    if len(argv) == 0:
        usage()
        return 0

    opts, _args = getopt.getopt(argv,"a:i:j:n:f:t:p:NvVl")
    option_dict={}
    for (option_name, option_value) in opts:
        option_dict[option_name] = option_value

    if '-a' in option_dict: # required parameter
        backupAttribute = option_dict['-a']
    else:
        backupAttribute = 'cs:backup'

    if '-N' in option_dict:
        dry_run=True
    else:
        dry_run=False

    if '-v' in option_dict:
        log.setPrintSeverity(log.VERBOSE)
        verbose = True
    else:
        log.setPrintSeverity(log.PROGRESS)
        verbose = False
    


    if '-n' in option_dict:
        email_address=option_dict['-n']
    else:
        email_address='<your email address here>'  # horrible hack


    command_found = False

    if '-i' in option_dict:
        # initialize
        command_found = True
        dataset = option_dict['-i']
        hostname, hostmac, poolname, ds_name = parse_poolordataset(dataset)

        info = f'Initialise -a {backupAttribute} '
        info += f'{ hostname+"::" if hostname else ""}{hostmac+"::" if hostmac else ""}'
        info += f'{poolname}/{ds_name}'
        info += f' {"(dry run)" if dry_run else ""}'
        log.add(info)
        if not dry_run:
            p=Pool(poolname,log=log)
            ds=p.dataset(ds_name)
            try:
                ds.attrSet(backupAttribute,'yes')
            except:
                nFails += 1
        
    if '-j' in option_dict and '-t' in option_dict: # single dataset backup
        if '-p' not in option_dict:
            error=f'Missing -p value'
            log.add(error,log.ERROR)
            nFails += 1
        else:
            command_found = True

            backupPrefix = option_dict['-p']
            fromhostname, frommac, frompoolname, from_ds_name = parse_poolordataset(option_dict['-j'])
            tohostname, tomac, topoolname, _to_ds_name  = parse_poolordataset(option_dict['-t'])
            fromPool=Pool(frompoolname,fromhostname,frommac,log=log,backupAttribute=backupAttribute,backupPrefix=backupPrefix)
            toPool=Pool(topoolname,tohostname,tomac,log=log,backupAttribute=backupAttribute,backupPrefix=backupPrefix)

            fromDS = fromPool.dataset(from_ds_name)
            toDS = toPool.dataset(from_ds_name)

            info = f'Backup dataset -a {backupAttribute} -p {backupPrefix} '
            # 
            info += f'Backup dataset {fromhostname + "::" if fromhostname else ""}{frommac + ":" if frommac else ""}{frompoolname}/{from_ds_name}'
            info += f' to {tohostname + "::" if tohostname else ""}{tomac + "::" if tomac else ""}{topoolname}'
            info += " (dry run)" if dry_run else ""

            if not dry_run:
                if frommac:
                    fromPool.wake()
                if tomac:
                    toPool.wake()

                try: # find setting of to attribute - if present
                    toAttr = toDS.attrGet(backupAttribute)
                except:
                    toAttr = None   # Dataset not exists

                try: 
                    fromAttr = fromDS.attrGet(backupAttribute)
                except:
                    fromAttr = None

                if toAttr == 'yes':
                    # Error - can't have source and destination both marked at backup masters
                    error=f'Destination pool has a matching dataset that already has the backup attribute set'
                    log.add(error,log.ERROR)
                    nFails += 1
                else:
                    # Mark as a backup source
                    if fromAttr != 'yes':
                        fromDS.attrSet(backupAttribute,'yes')

                    try:
                        fromDS.dsBackup(toPool)
                    except:
                        nFails += 1

                if frommac:
                    fromPool.sleep()
                if tomac:
                    toPool.sleep()


    if '-l' in option_dict: # List backup datasets
        if '-f' not in option_dict:
            error=f'Missing -f value'
            log.add(error,log.ERROR)
            nFails += 1
        else:
            command_found = True

            fromhostname, frommac, frompoolname, ds_name = parse_poolordataset(option_dict['-f'])
            fromPool=Pool(frompoolname,fromhostname,frommac,log=log,backupAttribute=backupAttribute)

            info = f'List backup datasets -a {backupAttribute} '
            # 
            info += f'from pool {fromhostname + "::" if fromhostname else ""}{frommac + ":" if frommac else ""}{frompoolname}'
            result=fromPool.getBackupDatasets()
            for ds in result:
                print (ds)
            

    if '-f' in option_dict and '-t' in option_dict:
        if '-p' not in option_dict:
            error=f'Missing -p value'
            log.add(error,log.ERROR)
            nFails += 1
        else:
            command_found = True

            backupPrefix = option_dict['-p']
            fromhostname, frommac, frompoolname, ds_name = parse_poolordataset(option_dict['-f'])
            tohostname, tomac, topoolname, ds_name  = parse_poolordataset(option_dict['-t'])
            fromPool=Pool(frompoolname,fromhostname,frommac,log=log,backupAttribute=backupAttribute,backupPrefix=backupPrefix)
            toPool=Pool(topoolname,tohostname,tomac,log=log,backupAttribute=backupAttribute,backupPrefix=backupPrefix)

            info = f'Backup pool -a {backupAttribute} -p {backupPrefix} '
            # 
            info += f'Backup pool {fromhostname + "::" if fromhostname else ""}{frommac + ":" if frommac else ""}{frompoolname}'
            info += f' to {tohostname + "::" if tohostname else ""}{tomac + "::" if tomac else ""}{topoolname}'
            info += " (dry run)" if dry_run else ""

            if not dry_run:
                if frommac:
                    fromPool.wake()
                if tomac:
                    toPool.wake()

                (_nSucceeds, nFails) = fromPool.poolBackup(toPool)
                if frommac:
                    fromPool.sleep()
                if tomac:
                    toPool.sleep()


    if not command_found:
        usage()
        return 1

    if (nFails > 0) or verbose:
        subject='Backup Failure' if (nFails > 0) else 'Backup Notification'
        if verbose:
            mail (log.getFilteredMessages(log.VERBOSE), subject=subject, to=email_address)
        else:
            message = log.getFilteredMessages(log.ERROR)
            message += '\n---------------------------------------\n'
            message = log.getFilteredMessages(log.INFO)
            mail (message, email_address, subject)            
        return 1

    return 0

if __name__ == '__main__':
    exit(main(sys.argv[1:]))
