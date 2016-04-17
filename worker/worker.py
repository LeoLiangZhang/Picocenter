'''A worker is a manager of picoprocesses. The current worker is built for
KVM picoprocesses.
'''

import sys, os
import time, subprocess, signal, threading

from urlparse import urlparse
import shutil
import urllib2

import netfilter
import re
import gzip

# OrderedDict
OrderedDict = None
try:
    import collections
    OrderedDict = collections.OrderedDict
except:
    import ordereddict
    OrderedDict = ordereddict.OrderedDict

from collections import deque

import boto, boto.utils

import config, iptables, hubproxy
logger = config.logger

import ujson
from datetime import datetime

from pprint import pprint

######################
## Helper functions ##
######################
def ensure_dir_exists(path):
    if not os.path.isdir(path):
        os.makedirs(path, mode=0775)

def open_input_output(path):
    '''Return a tuple (stdin, stdout, stderr) of file handlers in the path
    directory. Note: stdin is point to /dev/null.
    '''
    stdout_path = os.path.join(path, 'stdout')
    stderr_path = os.path.join(path, 'stderr')
    f_stdin = open('/dev/null', 'rb', 0) # no buffer
    f_stdout = open(stdout_path, 'wb')
    f_stderr = open(stderr_path, 'wb')
    return f_stdin, f_stdout, f_stderr

def time_diff(t1, t2):
    delta = t2-t1
    return ((delta.seconds * 1000) + (delta.microseconds / 1000))

########################
## Process Management ##
########################
class ProcessBase(object):
    """Provide basic control for process."""
    def __init__(self):
        self._popen = None
        self._args = []
        self._cwd = None
        self._env = None
        self._returncode = None
        self.stop_callback = None

    def append_args(self, obj):
        if isinstance(obj, dict):
            for k, val in obj.iteritems():
                self._args.append(str(k))
                self._args.append(str(val))
        elif isinstance(obj, list):
            self._args += obj
        else:
            self._args.append(str(obj))

    def _execute(self, args, stdin=None, stdout=None, stderr=None,
                close_fds=False, cwd=None, env=None):
        self._popen = subprocess.Popen(args, stdin=stdin, stdout=stdout,
                                       stderr=stderr, close_fds=close_fds,
                                       cwd=cwd, env=env)
        logger.debug('exec(%s) => %d', self._args, self._popen.pid)
        ProcessBase._add_process(self)

    def run(self):
        if self._cwd:
            ensure_dir_exists(self._cwd)
        f_stdin, f_stdout, f_stderr = open_input_output(self._cwd)
        self._execute(self._args,
                     stdin=f_stdin, stdout=f_stdout, stderr=f_stderr,
                     close_fds=True, cwd=self._cwd, env=self._env)
        f_stdin.close()
        f_stdout.close()
        f_stderr.close()

    @property
    def pid(self):
        if self._popen:
            return self._popen.pid
        else:
            return -1

    @property
    def stopped(self):
        return self.returncode is not None

    @property
    def returncode(self):
        if self._returncode is not None:
            return self._returncode
        elif self._popen:
            self._returncode = self._popen.poll()
        return self._returncode

    def kill(self):
        try:
            if self._popen:
                self._popen.kill();
        except:
            pass

    def send_signal(self, signal):
        if self._popen:
            self._popen.send_signal(signal)

    @classmethod
    def _setup_sigchld_handler(cls):
        ''' Setup child signal handler for process management.

        TODO: waitpid only return one of the stopped processes.
        May call it more than one time to get all. Also, should integrate
        with tornado.ioloop.
        http://www.tornadoweb.org/en/stable/_modules/tornado/process.html#Subprocess.initialize
        '''
        cls._processes = {}
        cls._process_stop_listeners = []
        def sigchld_handler(signum, frame):
            ret = None
            try:
                ret = os.waitpid(-1, os.WNOHANG)
            except OSError as e:
                if e.errno == 10: # No child processes.
                    return
                logger.debug('In sigchld_handler, os.waitpid() raised %s.', e)
                return
            logger.debug('os.waitpid() => %s', ret)
            if ret is None:
                return
            pid, rc = ret
            p = cls._processes.get(pid, None)
            if p:
                p._returncode = rc
                del cls._processes[pid]
                for listener in cls._process_stop_listeners:
                    try:
                        listener(p)
                    except Exception as e:
                        fmt = 'Error raised in process_stop_listener: %s'
                        logger.critical(fmt, e)
                if p.stop_callback:
                    try:
                        p.stop_callback(p)
                    except Exception as e:
                        fmt = 'Error raised in stop_callback: %s'
                        logger.critical(fmt, e)
        signal.signal(signal.SIGCHLD, sigchld_handler)

    @classmethod
    def _add_process(cls, p):
        pid = p.pid
        cls._processes[pid] = p

    @classmethod
    def add_process_stop_listener(cls, listener):
        cls._process_stop_listeners.append(listener)

    @classmethod
    def has_running_process(cls):
        return len(cls._processes) > 0

# Init SIGCHLD signal handler, and process_stop_listener service
ProcessBase._setup_sigchld_handler()


class FuseIPC:
    def __init__(self, pico_id, mnt, ipc_file_format, use_active_set):
        self.pico_id = pico_id
        self.mnt = mnt
        self.ipc =  os.path.join(mnt,ipc_file_format.format(pico_id=pico_id))
        self.use_active_set = use_active_set
        #self.pages_log = os.path.join(mnt, str(pico_id), 'pages.log')
        self.pages_log = os.path.join('/tmp/pico_cache/', str(pico_id), 'pages.log')
        self.as_file_format = os.path.join(self.mnt,str(self.pico_id),'{0}','as')
        logger.info("Pages log located at %s", self.pages_log)

    def send_cmd(self, cmd):
        """
        Write will block until operation is complete. This command
        should only be run in a separate thread to ensure that the main
        execution loop isn't blocked.
        """
        logger.info("sending cmd %s to fuse", cmd)
        with open(self.ipc + "." + str(threading.current_thread().ident), 'w', 0) as f:
        #with open(self.ipc, 'a+') as f:
            f.write(cmd)
            #f.flush()
        logger.info("writing to fuse returned")

    def clear_cache(self):
        self.send_cmd("delete-cache")

    def read_as(self, exec_count):
        logger.info("reading as")
        with open(self.as_file_format.format(exec_count), 'r+') as f:
        #with open(os.path.join(self.mnt,self.pico_id,exec_count,'as'), 'r+') as f:
            serialized_as = f.read()
        return serialized_as

    def write_as(self, exec_count, serialized_as):
        logger.info("writing as")
        with open(self.as_file_format.format(exec_count), 'w+') as f:
        #with open(os.path.join(self.mnt,self.pico_id,exec_count,'as'), 'w+') as f:
            f.write(serialized_as)

    def _run_in_new_thread(self, func, args):
        logger.info("pico.fuse._run_in_new_thread, func=%s",func.__name__)
        thread = threading.Thread(target=func, args=args)
        thread.start()
        return thread

    def download_checkpoint(self, exec_count):
        """
        exec_count is number of times this process has been run previously
        we need to download directories 1->exec_count
        """
        threads = []
        #dirs = range(1,exec_count+1)
        dirs = [exec_count]
        if self.use_active_set == "True":
            dirs += ['as']
        for i in range(1,exec_count):
            os.makedirs("/tmp/pico_cache/{0}/{1}".format(self.pico_id, i))
        for i in dirs:
            logger.info("Starting thread for " + "{0}".format(i))
            threads.append(self._run_in_new_thread(self.send_cmd, ["download {0}".format(i)]))
            logger.info("Done starting thread for " + "{0}".format(i))
            # self.send_cmd("download {0}".format(i))

        logger.info("About to join on threads")
        map(lambda x: x.join(), threads)
        logger.info("Done join on threads")

    def upload_checkpoint(self, init_exec, curr_exec):
        """
        init_exec: number of times process ran elsewhere
        curr_exec: exec_count, diff shows how many ckpt directories
        we created on this machine and thus how many we shoud upload
        """
        logger.info("uploading checkpoints to fuse (%d:%d)", init_exec, curr_exec)
        #dirs = range(init_exec+1, curr_exec+1)
        dirs = [curr_exec]
        if self.use_active_set == "True":
            dirs += ['as']
        for i in dirs:
            self.send_cmd("upload {0}".format(i))
        logger.info("upload finished")

    def upload_one_checkpoint(self, one):
        cmd = "upload {0}".format(one)
        logger.info("send one upload command to fuse: %s", cmd)
        self.send_cmd(cmd)
        logger.info("upload finished")

    def upload_as_folder(self):
        self.upload_one_checkpoint("as")

    def get_page_requests(self, delete=True):
        """
        Read pages.log to get list of pages accessed since the last time
        we checked (i.e. since the last checkpoint). Should be in the form...
            (checkpoint_dir, page_file, size, offset)
        ex: (ckpt1, pages-1.img, 4096, 0)
        """
        logger.info("===> getting pages from pages.log")
        p = []
        try:
            with open(self.pages_log, 'r+') as f:
                lines = f.readlines()
                for l in lines:
                    t, path, size, offset = l.strip().split(" ")
                    logger.debug("%s %s", t, path)
                    ckpt, page_file = path.split("/")
                    p.append((ckpt, page_file, int(size), int(offset)))
            # Remove it before we go on so that page server will start over
            logger.info("finished getting pages")
            if delete:
                os.remove(self.pages_log)
        except IOError as e:
            logger.warn(e)
            logger.warn("failed getting pages")
        logger.debug("get_page_requests() -> "+str(p))
        return p

    # Called first to prefetch common pages
    # Called again on connection to fetch any more pages for specific ports
    # TODO should be called from some iptables log listener function
    def prefetch(self, pages):
        """
        pages : ("page-x.img",offset)
        """
        #pages = [("1","pages-3.img",1,1),("2","pages-1.img",1,1)]
        logger.info("prefetching...")
        for (ckpt, page_file, size, offset) in pages:
            with open(os.path.join(self.mnt,str(self.pico_id),ckpt,page_file),'r+') as f:
                f.seek(int(offset))
                f.read(1)
        logger.info("done prefetching")

re_page_meta = re.compile('pages-[0-9]*\.img\.meta$')
re_page_file = re.compile('pages-[0-9]*\.img$')
class ASManager:
    def __init__(self, pico, algorithm):
        self.pico = pico
        self.block_size = pico.config.block_size
        self.algorithm = algorithm
        if algorithm == "dumb":
            self.data = {'last_used' : None}
            self.add = self._dumb_add
            self.get = self._dumb_get
            logger.info("Using dumb ActiveSet algorithm")
        elif algorithm == "kn":
            self.add = self._kn_add
            self.get = self._kn_get
            self.pagemap = {}
            logger.info("Using KN ActiveSet algorithm")
        elif algorithm == "none":
            self.add = self._none_add
            self.get = self._none_get
            logger.info("Using none ActiveSet algorithm")

        self.as_dir = '/tmp/pico_cache/{0}/as/'.format(self.pico.pico_id)
        self.as_format = '/tmp/pico_cache/'+str(self.pico.pico_id)+'/as/{0}-{1}-{2}'
        self.part_format = '/tmp/pico_cache/'+str(self.pico.pico_id)+'/{0}/{1}.{2}.part'
        self.full_format = '/tmp/pico_cache/'+str(self.pico.pico_id)+'/{0}/{1}'
        self.as_file_format = '/tmp/pico_cache/'+str(self.pico.pico_id)+'/as/{0}'
        ensure_dir_exists('/tmp/as')

    def _ensure_as_folder(self):
        if not os.path.exists(self.as_dir):
            os.makedirs(self.as_dir)

    def copy_page_meta(self, exec_count):
        ''' Copy all page meta files from given checkpoint folder to as folder.

        exec_count (int): checkpoint folder number
        '''
        # liang: call this function after each upload.
        # currently fuse only creates page meta when upload is called
        self._ensure_as_folder()
        checkpoint_dir = '/tmp/pico_cache/{0}/{1}/'.format(self.pico.pico_id, exec_count)
        for filename in os.listdir(checkpoint_dir):
            m = re_page_meta.match(filename)
            if m:
                # copy page meta file
                dst = self.as_format.format(exec_count, filename, "M")
                src = os.path.join(checkpoint_dir, filename)
                data = None
                with open(src, 'rb') as f:
                    data = f.read()
                with open(dst, 'wb') as f:
                    f.write(data)
                logger.debug("copy_page_meta: copy %s to %s", src, dst)


    #################
    ###### None #####
    #################
    def _none_add(self, ports, pages, exec_count):
        pass
    def _none_get(self, exec_count):
        return []

    #################
    ###### DUMB #####
    #################
    def _dumb_add(self, ports, pages, exec_count):
        self.data['last_used'] = pages
    def _dumb_get(self, exec_count):
        return self.data['last_used']

    #################
    #### K of N #####
    #################
    def _kn_add(self, ports, pages, exec_count):
        for page in pages:
            if page in self.pagemap:
                self.pagemap[page].append(exec_count)
            else:
                self.pagemap[page] = [exec_count]

    def _kn_get(self, exec_count):
        return filter(lambda page : len(filter(lambda x : x > (exec_count - self.n), self.pagemap[page])) >= self.k, self.pagemap)

    def flush(self, exec_count):
        logger.debug("STARTING FLUSH")
        # subprocess.call(['mkdir','-p','/tmp/pico_cache/{0}/as/'.format(self.pico.pico_id)])
        self._ensure_as_folder()
        activeset = self.get(exec_count)
        # pprint(activeset)
        if activeset:
            for (ckpt,page_file,size,offset) in activeset:
                first_block = int(offset / self.block_size)
                last_block = int((offset+size) / self.block_size)
                logger.debug('page_file: %s, first_block=%d, last_block=%d', page_file, first_block, last_block)
                # with open('/tmp/pico_cache/{0}/{1}/{2}'.format(self.pico.pico_id,ckpt,page_file)) as f:
                with open('/checkpoints/{0}/{1}/{2}'.format(self.pico.pico_id,ckpt,page_file)) as f:
                    for block in range(first_block,last_block+1):
                        f.seek(block * self.block_size)
                        buf = f.read(self.block_size)
                        outfile = self.as_format.format(ckpt,page_file,block)
                        logger.debug('writing %s', outfile)
                        out = open(outfile,'w')
                        out.write(buf)
                        out.close()

        # Also add latest checkpoint data to active set
        if self.algorithm != 'none':
            latest_checkpoint_dir = '/tmp/pico_cache/{0}/{1}/'.format(self.pico.pico_id,exec_count)
            for filename in os.listdir(latest_checkpoint_dir):
                m = re_page_file.match(filename)
                # if filename[:6] == 'pages-':
                if m:
                    os.rename(latest_checkpoint_dir+filename,self.as_format.format(exec_count,filename,"A"))

        # liang: save pages-*.img.meta files
        # Assuming we name checkpoint folders as 1, 2, ..., N

        # liang: this function has moved to copy_page_meta()
        # prev_exec_count = exec_count - 1
        # prev_checkpoint_dir = '/tmp/pico_cache/{0}/{1}/'.format(self.pico.pico_id,prev_exec_count)
        # if not os.path.exists(prev_checkpoint_dir):
        #     return
        # for filename in os.listdir(prev_checkpoint_dir):
        #     m = re_page_meta.match(filename)
        #     if m:
        #         os.rename(latest_checkpoint_dir+filename,self.as_format.format(prev_exec_count,filename,"M"))

        # move all previous page meta files back to current as folder
        temp_dir = '/tmp/as/{0}'.format(self.pico.pico_id)
        if os.path.exists(temp_dir):
            for filename in os.listdir(temp_dir):
                if filename.endswith('.img.meta-M'):
                    src = os.path.join(temp_dir, filename);
                    os.rename(src, self.as_file_format.format(filename))

        # these lines for debug purpose:
        logger.debug("List folder %s", self.as_dir)
        for filename in os.listdir(self.as_dir):
            logger.debug(os.path.join(self.as_dir, filename))

    def restore(self):
        temp_dir = '/tmp/as/{0}'.format(self.pico.pico_id)
        # os.system('rm -rf {1} && mv {0} {1}'.format(self.as_dir, temp_dir))
        try:
            shutil.rmtree(temp_dir)
        except OSError as e:
            pass
        os.rename(self.as_dir, temp_dir)
        for filename in os.listdir(temp_dir):
            ckpt, pages, file_num, block = filename.split('-')
            page_file = '-'.join([pages, file_num])
            source = temp_dir + '/' + filename
            if block == "A":
                dest = self.full_format.format(ckpt,page_file)
            elif block == "M":
                dest = self.full_format.format(ckpt,page_file)
                dest_dir = os.path.dirname(dest)
                ensure_dir_exists(dest_dir)
            else:
                dest = self.part_format.format(ckpt,page_file,block)
            if not os.path.exists(dest):
                # logger.info("moving %s to %s",source,dest)
                os.symlink(source,dest)
            # else:
                # we are in the latest ckpt folder, which already has pages.meta files
                # logger.info("skip moving %s to %s",source,dest)


#################
## Picoprocess ##
#################
class Pico:

    ### STARTING STATES
    """
    Pico object has been creating, but no methods have been called yet
    """

    # Starting for first time (exec_count=0) or from cold (exec_count>0)
    INIT = 0

    ### PROGRESS STATES
    """
    Some operation is in progress, likely in a separate thread.
    Need to be careful. Not ready to change states.
    """

    # Waiting for container to be ready to accept packets
    STARTING = 10
    # Waiting for container to finish checkpointing.
    CHECKPOINTING = 11
    # Waiting for container to stop running
    STOPPING = 12
    # Waiting for container to finish being uploaded to S3
    UPLOADING = 13
    # Waiting for S3 to finish fetching file
    DOWNLOADING = 14


    ### STEADY STATES
    """
    Worker not currently doing anything for this process. Ready to change states.
    """
    # Process inside container is ready to accept packets from the network
    RUNNING = 20
    # Process is stopped, but we have everything we need to run it again
    STOPPED = 21
    # LXC is unaware of the process, it only exists as an object here in the
    # worker
    COLD = 22
    CHECKPOINTED = 23

    ### ERROR STATES
    FAILED_TO_START = 30
    FAILED_TO_CHECKPOINT = 31
    FAILED_TO_STOP = 32


    def __init__(self, config, pico_id, internal_ip=None, portmaps=[], exec_count=0):
        self.config = config
        self.pico_id = pico_id
        self.internal_ip = internal_ip
        self.portmaps = portmaps
        self.init_exec = exec_count
        self.exec_count = exec_count # number of times this pico has been run globally
        self.status = Pico.INIT
        self.asmanager = ASManager(self, config.as_algorithm)
        if config.as_algorithm == "kn":
            self.asmanager.k = config.as_k
            self.asmanager.n = config.as_n

        self.checkpoint_callback = None
        self.release_callback = None
        self.cleanup_callback = None

        # should_alive: None if not set, False if killed, True if ensure_alive
        self.should_alive = None
        self.timing = {}
        self.profiling = {}  # liang: use this dict to save profiling related vars

        # LXC
        self.container_name = "pico"+str(pico_id)
        self.checkpoint_base = os.path.join(self.config.lxc.checkpoint_dir.format(pico_id=pico_id),'{exec_count}')
        self.checkpoint_dir = self.checkpoint_base.format(exec_count=exec_count)
        self.filesystem_snap = self.config.lxc.filesystem_snap.format(pico_id=pico_id)
        path_to_user_init = "/bin/ash"
        user_args = "/root/init-{0}.sh".format(self.config.app_name)
        self.user_init_cmd = (path_to_user_init + " " + user_args).strip().split(" ")
        #self.user_init_cmd = ["/bin/ash","-c","(/root/test </dev/null 2>&1 1>/dev/null &)"]
        self.pico_path = config.lxc.pico_path.format(pico_id=pico_id)
        self.rootfs_path = os.path.join(self.pico_path, 'rootfs')
        self.config_path = os.path.join(self.pico_path, 'config')

        # Fuse
        self.fuse = FuseIPC(pico_id, config.lxc.fuse_mount_dir, config.lxc.fuse_ipc_format, config.use_active_set)

    def _increment_exec_count(self):
        logger.info("START: pico._increment_exec_count, %d+1", self.exec_count)
        self.exec_count += 1
        self.checkpoint_dir = self.checkpoint_base.format(exec_count=self.exec_count)
        logger.debug("EXIT: new checkpoint_dir=%s",self.checkpoint_dir)

    def _init(self):
        logger.info("START: pico._init, state=%d",self.status)

        self.should_alive = None
        self.status = Pico.STARTING

        # Create new container
        self.timing['spawn_start'] = datetime.now()
        ret = subprocess.call([
            self.config.lxc.spawn_script,
	    str(self.pico_id)
            #self.container_name,
            #self.internal_ip,
            #self.config.lxc.network_mask,
            #self.config.lxc.gateway
        ])
        self.timing['spawn_stop'] = datetime.now()

        self.timing['init_start'] = datetime.now()
        ret = subprocess.call([
            'cp',
            '/elasticity/scripts/init-{0}.sh'.format(self.config.app_name),
            #'/elasticity/experiment/alan-test/test',
            os.path.join(self.rootfs_path,'root')
        ])

        ret = subprocess.call([
            'cp',
            '/elasticity/experiment/swap/1k',
            #'/elasticity/scripts/10k',
            #'/elasticity/scripts/100k',
            #'/elasticity/scripts/1000k',
            os.path.join(self.rootfs_path,'var', 'www', 'localhost', 'htdocs', 'index.html')
        ])

        if self.config.app_name == "clamd":
            # logger.info("sleeping...")
            time.sleep(3)
        # Start the users application
        ret = subprocess.call([
            "lxc-attach",
            "-n",
            self.container_name,
            "--"
        ] + self.user_init_cmd)
        self.timing['init_done'] = datetime.now()

        ensure_dir_exists(os.path.join(self.config.lxc.fuse_mount_dir,str(self.pico_id)))
        self._increment_exec_count()

        #TODO Need to do this so that fuse can start tracking pages???
        #TODO CHECK THAT EVERYTHING WAS SUCCESSFUL...
        #self._checkpoint(first=True)
        #self._restore(first=True)
        self.status = Pico.RUNNING
        logger.info("EXIT: pico._init, state=%d",self.status)

    def _init_from_cold(self):
        logger.info("START: pico._init_from_cold, state=%d",self.status)

        self.status = Pico.DOWNLOADING

        # TODO self.fuse.download_fs() # puts in self.fs_dir
        # TODO The new two could be done in a separate thread?
        self.timing['download_checkpoint_start'] = datetime.now()
        self.fuse.download_checkpoint(self.exec_count)
        self.timing['download_checkpoint_stop'] = datetime.now()
        # liang: during release, btrfs snapshot is saved to the AS folder
        # see release for details
        os.rename(self.config.lxc.fmt_btrfs_snap.format(self.pico_id, self.exec_count), self.filesystem_snap)

        # Do this async
        self.timing['btrfs_recieve_start'] = datetime.now()
        logger.info("calling btrfs restore")
        ret = subprocess.call([
            self.config.lxc.restore_fs_script,
            self.config.lxc.lxcpath,
            str(self.pico_id)
        ])
        logger.info("returned from btrfs restore")
        self.timing['bind_mount_stop'] = datetime.now()
        self.timing['read_as_start'] = datetime.now() #self.timing['download_checkpoint_stop']
        if self.config.use_active_set == "True":
            self.asmanager.restore()
        self.timing['read_as_stop'] = datetime.now()
        logger.info("returned from asmanager.restore")
        self.timing['prefetch_start'] = self.timing['read_as_stop']
        # NO PREFETCHING ANYMORE!
        self.timing['prefetch_stop'] = datetime.now()
        # TODO WHAT IF WE NEED TO CHANGE THE IP? (basically just need to run the
        # networking portion of spawn_container agian, iptables should already be taken
        # care of if given the correct stuff in function call)
        if self.config.profile_downloads:
            self.profiling['download_init'] = self.get_download_size()
        self._restore()
        if self.config.profile_downloads:
            self.profiling['download_restore'] = self.get_download_size()
        logger.info("EXIT: pico._init_from_cold, state=%d",self.status)

    def _restore(self):
        logger.info("START: pico._restore, state=%d (pico_id=%d)", self.status, self.pico_id)

        self.status = Pico.STARTING

        self.timing['restore_start'] = datetime.now()
        logger.info("calling lxc-checkpoint RESTORE from %s", self.checkpoint_dir)
        ret = subprocess.call([
            'lxc-checkpoint',
            '-r',
            '-n',
            self.container_name,
            #'-v',
            '-D',
            self.checkpoint_dir
        ])
        self.timing['restore_stop'] = datetime.now()

        self._increment_exec_count()
        self.status = Pico.RUNNING
        logger.info("EXIT: pico._restore, state=%d (pico_id=%d)", self.status, self.pico_id)

    def _run_in_new_thread(self, func):
        # liang: disable threading in class Pico for sync experiments
        # TODO: design a better arch to support parallel jobs
        # logger.info("pico._run_in_new_thread, func=%s",func.__name__)
        # thread = threading.Thread(target=func)
        # thread.start()
        # return thread
        func();

    def execute(self):
        logger.info("START: pico.execute, state=%d (pico_id=%d)", self.status, self.pico_id)
        # We're already running, don't do anything
        if self.status == Pico.RUNNING:
            logger.info("EXIT: pico.execute, pico already running")
            return

        elif self.status == Pico.INIT:
            if self.exec_count > 0: # (cold -> hot)
                logger.info("moving cold->hot")
                self._run_in_new_thread(self._init_from_cold)
            else: # (init -> hot)
                logger.info("moving init->hot")
                self._run_in_new_thread(self._init)

        elif self.status == Pico.CHECKPOINTED:
            logger.info("moving warm->hot")
            self._run_in_new_thread(self._restore)

        logger.info("EXIT: pico.execute, state=%d (pico_id=%d)", self.status, self.pico_id)

    def _checkpoint(self):
        logger.info("START: pico._checkpoint, state=%d (pico_id=%d)", self.status, self.pico_id)
        self.status = Pico.CHECKPOINTING

        ensure_dir_exists(self.checkpoint_dir)
        self.timing['checkpoint_start'] = datetime.now()
        ret = subprocess.call([
            'lxc-checkpoint',
            '-s',
            '-n',
            self.container_name,
            #'-v',
            '-D',
            self.checkpoint_dir
        ])
        self.timing['checkpoint_stop'] = datetime.now()

        if self.config.use_active_set == "True":
            if self.exec_count > 1:
                self.timing['get_pages_start'] = datetime.now()
                page_requests = self.fuse.get_page_requests()
                self.timing['get_pages_stop'] = datetime.now()

                # TODO GET PORTS USED
                ports = set([80,443])
                self.timing['update_as_start'] = datetime.now()
                self.asmanager.add(ports, page_requests, self.exec_count)
                self.timing['update_as_stop'] = datetime.now()

            #liang: todo: upload checkpoint folder
            self.fuse.upload_one_checkpoint(self.exec_count)
            self.asmanager.copy_page_meta(self.exec_count)

        if self.checkpoint_callback:
            logger.info("calling checkpoint_callback")
            self.checkpoint_callback(self)

        self.status = Pico.CHECKPOINTED
        logger.info("EXIT: pico._checkpoint, state=%d (pico_id=%d)", self.status, self.pico_id)

    def checkpoint(self):
        logger.info("pico.checkpoint, state=%d (pico_id=%d)", self.status, self.pico_id)
        if self.status == Pico.RUNNING:
            self._run_in_new_thread(self._checkpoint)

    def _kill(self):
        logger.info("_kill, state=%d", self.status)

        ret = subprocess.call(['lxc-stop', '-n', self.container_name])
        self.status = Pico.STOPPED

    def kill(self):
        logger.info("kill, state=%d", self.status)
        if self.status == Pico.RUNNING:
            self.status = Pico.STOPPING
            self._run_in_new_thread(self._kill)
        else:
            logger.warn("tried to kill pico_id=%d, but status is %d",
                    self.pico_id, self.status)

    def _cleanup(self):
        logger.info("START: pico._cleanup, state=%d (pico_id=%d)", self.status, self.pico_id)

        self.timing['btrfs_delete_start'] = datetime.now()
        ret = subprocess.call([
            'btrfs',
            'subvolume',
            'delete',
            os.path.join(self.config.lxc.lxcpath,self.container_name)
        ])
        self.timing['btrfs_delete_stop'] = datetime.now()

        self.timing['clear_cache_start'] = datetime.now()
        self.fuse.clear_cache()
        tmp_as = "/tmp/as/{0}".format(self.pico_id)
        if os.path.exists(tmp_as):
            shutil.rmtree(tmp_as)
        self.timing['clear_cache_stop'] = datetime.now()

        if self.cleanup_callback:
            logger.info("calling cleanup callback")
            self.cleanup_callback(self)

    def cleanup(self):
        logger.info("pico.cleanup, state=%d (pico_id=%d)", self.status, self.pico_id)
        if self.status == Pico.COLD:
            self._run_in_new_thread(self._cleanup)
        else:
            logger.warn("Failed to cleanup pico_id=%d, pico must be cold",
                    self.pico_id)

    def _release(self):
        logger.info("START: pico._release, state=%d (pico_id=%d)", self.status, self.pico_id)
        self.status = Pico.UPLOADING

        self.timing['unmount_start'] = datetime.now()
        ret = subprocess.call([
            'umount',
            os.path.join(self.rootfs_path, 'checkpoints')
        ])
        self.timing['unmount_stop'] = datetime.now()

        # Call an explicit FS sync because btrfs fucking sucks
        # https://btrfs.wiki.kernel.org/index.php/Incremental_Backup
        ret = subprocess.call(['sync'])

        self.timing['btrfs_send_start'] = datetime.now()
        ret = subprocess.call([
            'btrfs',
            'property',
            'set',
            '-ts',
            self.pico_path,
            'ro',
            'true'
        ])

        ret = subprocess.call([
            'btrfs',
            'send',
            '-p',
            self.config.lxc.base_path,
            self.pico_path,
            '-f',
            self.filesystem_snap
        ])
        # liang: move filesystem snap to AS dir
        # NOTE: rename cannot move file across device, so used fuse internal cache folder here
        # be careful, if cache folder is on, say tmpfs, then this call may break!
        os.rename(self.filesystem_snap, self.config.lxc.fmt_btrfs_snap.format(self.pico_id, self.exec_count))
        self.timing['btrfs_send_stop'] = datetime.now()

        self.timing['write_as_start'] = datetime.now()
        self.asmanager.flush(self.exec_count)
        self.timing['write_as_stop'] = datetime.now()

        if self.config.profile_uploads:
            self.profiling['upload_ckpt'] = self.get_upload_size()
        
        self.timing['upload_start'] = datetime.now()
        if self.config.use_active_set == "True":
            self.fuse.upload_as_folder()
        else:
            self.fuse.upload_checkpoint(self.init_exec, self.exec_count)
        self.timing['upload_stop'] = datetime.now()

        self.status = Pico.COLD

        if self.release_callback:
            logger.info("calling release callback")
            self.release_callback(self)
        logger.info("START: pico._release, state=%d (pico_id=%d)", self.status, self.pico_id)

    def release(self):
        logger.info("release, state=%d", self.status)
        if self.status == Pico.RUNNING:
            logger.warn("pico %d is still running, must be stopped to be destroyed")
        elif self.status == Pico.CHECKPOINTED:
            self._run_in_new_thread(self._release)

    def get_upload_size(self):
        pico_id = self.pico_id
        exec_count = self.exec_count
        pico_dir = '/tmp/pico_cache/{0}/'.format(pico_id)
        raw_sz = 0
        dirs = [str(exec_count)]
        if self.config.use_active_set == "True":
            dirs.append('as')
        for i in dirs:
            ckpt_dir = os.path.join(pico_dir, i)
            # if not os.path.exists(ckpt_dir):
            #     continue
            for name in os.listdir(ckpt_dir):
                p = os.path.join(ckpt_dir, name)
                s = os.stat(p)
                raw_sz += s.st_size
                logger.debug('[get_upload_size] %s %s %s', raw_sz, s.st_size, p)
        return raw_sz

    def get_download_size(self):
        pico_id = self.pico_id
        exec_count = self.exec_count
        pico_dir = '/tmp/pico_cache/{0}/'.format(pico_id)
        # _debug_line ='[get_download_size(pico_id={0}, exec_count={1})]'.format(pico_id, exec_count) 
        # logger.debug(_debug_line)
        raw_sz = 0
        for i in range(1, exec_count+1, 1):  # include current ckpt dir
            ckpt_dir = os.path.join(pico_dir, str(i))
            if not os.path.exists(ckpt_dir):
                continue
            for name in os.listdir(ckpt_dir):
                p = os.path.join(ckpt_dir, name)
                s = os.stat(p)
                raw_sz += s.st_size
                logger.debug('[get_download_size] %s %s %s', raw_sz, s.st_size, p)
        return raw_sz

    def get_download_size2(self):
        ''' Similar to get_download_size, except that this function tries to compress
        downloaded files, and reported zipped size as well as raw size.
        '''
        pico_id = self.pico_id
        exec_count = self.exec_count
        pico_dir = '/tmp/pico_cache/{0}/'.format(pico_id)
        # _debug_line ='[get_download_size2(pico_id={0}, exec_count={1})]'.format(pico_id, exec_count) 
        # logger.debug(_debug_line)
        gzip_filename = '/tmp/pico{0}.{1}.gz'.format(pico_id, exec_count)
        raw_sz = 0
        with gzip.open(gzip_filename, 'wb') as fout:
            for i in range(1, exec_count+1, 1):
                ckpt_dir = os.path.join(pico_dir, str(i))
                if not os.path.exists(ckpt_dir):
                    continue
                for name in os.listdir(ckpt_dir):
                    p = os.path.join(ckpt_dir, name)
                    with open(p, 'rb') as f:
                        data = f.read()
                        raw_sz += len(data)
                        fout.write(data)
                        logger.debug('[get_download_size2] %s %s %s', raw_sz, len(data), p)
        s = os.stat(gzip_filename)
        gzip_sz = s.st_size
        os.remove(gzip_filename)
        return raw_sz, gzip_sz


class PicoManager:
    def __init__(self, config, pico_exit_callback=None):
        self.config = config
        self._picos = {} # pico_id -> pico
        self.pico_exit_callback = pico_exit_callback
        ProcessBase.add_process_stop_listener(self._process_stop_listener)

    def _process_stop_listener(self, p):
        logger.critical("!=======! PROCESS_STOP_LISTENER, %s", str(p))
        pid = p.pid # p is the monitor
        if not hasattr(p, 'pico'):
            return
        monitor = p
        pico = p.pico
        if pico.status == Pico.CHECKPOINT:
            pico.status = Pico.STOPPED
        else:
            pico.status = Pico.EXIT
        pico.monitor = None
        if pico.checkpoint_callback:
            pico.checkpoint_callback(pico)
            # should I reset checkpoint_callback?
        if pico.status == Pico.STOPPED and pico.should_alive:
            self._resume(pico)
        elif pico.status == Pico.EXIT:
            # got killed or pico exited
            self.pico_exit_callback(pico)
            # self.release(pico.pico_id)

    def get_pico_by_id(self, pico_id):
        return self._picos.get(pico_id, None)

    def _delete(self, pico_id):
        if pico_id in self._picos:
            del self._picos[pico_id]

    def _execute(self, pico):
        pico.execute()
        logger.info('pico.execute(pico_id=%d, internal_ip=%s, exec_count=%d)'+
                    ' => pid=?', pico.pico_id, pico.internal_ip, pico.exec_count)
                    #TODO pico.pid)

    def _resume(self, pico):
        pico.resume = True
        self._execute(pico)

    def execute(self, pico_id, internal_ip, portmaps, exec_count):
        pico = self.get_pico_by_id(pico_id)
        if pico is None:
            pico = Pico(self.config, pico_id, internal_ip, portmaps, exec_count)
            self._picos[pico_id] = pico
            # try to call PortMapper.add_pico(pico_id) here, but first, we 
            # need to pass in portmapper object
            self._execute(pico)
        else:
            msg = 'PicoManager.execute fail: Pico(id=%s, status=%s) exists.'
            # use ensure_alive or release then execute
            logger.warn(msg, pico_id, pico.status)

    def checkpoint(self, pico_id, callback=None):
        pico = self.get_pico_by_id(pico_id)
        if pico is None:
            logger.warn('PicoManager.checkpoint: pico(id=%s) not found.',
                        pico_id)
            return
        pico.checkpoint_callback = callback
        if pico.status == Pico.STOPPED:
            if callback:
                callback(pico)
        else:
            pico.checkpoint()

    def kill(self, pico_id):
        pico = self.get_pico_by_id(pico_id)
        if pico is None:
            logger.warn('PicoManager.kill: pico(id=%s) not found.', pico_id)
            return
        pico.should_alive = False
        pico.kill()
        del self._picos[pico_id]

    def rm_pico_dir(self, pico_id):
        fmt = self.config.pico_path_fmt
        pico_dir = fmt.format(pico_id, '')
        shutil.rmtree(pico_dir, ignore_errors=True)

    # returns exec_count of picoprocess
    def release(self, pico_id, clean):

        pico = self.get_pico_by_id(pico_id)
        if pico is None:
            logger.warn('PicoManager.release: pico(id=%s) not found.',
                        pico_id)
            return -1
        if pico.should_alive:
            logger.warn('PicoManager.release: pico(id=%s) was not released,\
                        should_alive=True', pico_id)
            return -1

        # If this flag is set, clear out every trace of this pico on the system
        # to free up resources
        if clean:
            # Once we've released, let's lxc-destroy (i.e. delete rootfs)
            def release_callback(pico):
                logger.info("release callback")
                pico._cleanup()
            pico.release_callback = release_callback

            # Once we've released everything, we can delete any worker metadata
            def cleanup_callback(pico):
                logger.info("cleanup callback")
                # TODO how to delete from fuse?
                self._delete(pico.pico_id)
            pico.cleanup_callback = cleanup_callback
        else:
            def release_callback(pico):
                self._delete(pico.pico_id)
            pico.release_callback = release_callback

        # If process is running, checkpoint it first, then release
        if pico.status == Pico.RUNNING:
            def checkpoint_callback(pico):
                logger.info("checkpoint callback")
                pico._release()
            pico.checkpoint_callback = checkpoint_callback
            pico.checkpoint()
            exec_count = pico.exec_count
        # If process has already been checkpointed, we can just release now
        elif pico.status == Pico.CHECKPOINTED:
            pico.release()
            exec_count = pico.exec_count
        else:
            logger.warn("PicoManager.release: pico(id=%s), was not released,\
                        status was not running or checkpointed (was %d)",
                        pico_id, pico.status)
            exec_count = -1

        logger.debug('PicoManager.release %s finished (exec_count=%d).', pico_id, exec_count)
        return exec_count

    def ensure_alive(self, pico_id):
        # TODO: This funciton needs more testing, especially CHECKPOINT case
        pico = self.get_pico_by_id(pico_id)
        if pico is None:
            logger.warn('PicoManager.ensure_alive: pico(id=%s) not found.',
                        pico_id)
            return # pico not exist, ignore
        if pico.status == Pico.RUNNING:
            return
        self._resume(pico)

    def clear(self, pico_id):
        logger.info("worker cleaning up pico_id=%d",pico_id)
        pico = self.get_pico_by_id(pico_id)
        pico._kill()
        pico._cleanup()


class PortMapper:
    def __init__(self, worker):
        self.mapping = {}
        self.worker = worker
        # TODO, wont always be local
        self.local = False
        self.cfqueue = netfilter.FlowControlQueue(worker)
        self.cfqueue.async_run_nfqueue()

    def add_mappings(self, pico_id, portmaps):
        for portmap in portmaps:
            port_key = portmap.port_key
            self.mapping[port_key] = pico_id

    def remove_mappings(self, pico_id):
        pico = self.worker.picoman.get_pico_by_id(pico_id)
        portmaps = pico.portmaps
        for portmap in portmaps:
            port_key = portmap.port_key
            del self.mapping[port_key]

    def add_pico(self, pico_id, portmaps):
        #pico = self.worker.picoman.get_pico_by_id(pico_id)
        #portmaps = pico.portmaps
        #pico_id = pico.pico_id
        self.add_mappings(pico_id, portmaps)
        self.install_dnat(pico_id, portmaps)

    def remove_pico(self, pico_id):
        self.delete_dnat(pico_id)
        self.delete_log(pico_id) # TODO: is it necessary?
        self.remove_mappings(pico_id)

    def find(self, port_key):
        return self.mapping.get(port_key, None)

    def _dnat_func(self, pico_id, func, local, portmaps=None):
        if not portmaps:
            pico = self.worker.picoman.get_pico_by_id(pico_id)
            portmaps = pico.portmaps
        for portmap in portmaps:
            dip, dport, proto, tip, tport = (portmap.dip, portmap.dport,
                portmap.proto, portmap.tip, portmap.tport)
            func(dip, dport, proto, tip, tport, local)

    def install_dnat(self, pico_id, portmaps=None):
        self._dnat_func(pico_id, iptables.dnat, self.local, portmaps)

    def delete_dnat(self, pico_id):
        self._dnat_func(pico_id, iptables.delete_dnat, self.local)

    def _log_func(self, pico_id, func, local):
        pico = self.worker.picoman.get_pico_by_id(pico_id)
        portmaps = pico.portmaps
        for portmap in portmaps:
            dip, dport, proto = (portmap.dip, portmap.dport, portmap.proto)
            func(dip, dport, proto)

    def _get_portmaps(self, pico_id):
        pico = self.worker.picoman.get_pico_by_id(pico_id)
        portmaps = pico.portmaps
        return portmaps

    def install_log(self, pico_id, portmaps=None):
        # switching to nfqueue
        # self._log_func(pico_id, iptables.log, self.local)
        if not portmaps:
            portmaps = self._get_portmaps(pico_id)
        for portmap in portmaps:
            dip, dport, proto = (portmap.dip, portmap.dport, portmap.proto)
            iptables.install_queue(dip, dport, proto) # TODO does this sequence matter?
            self.cfqueue.block(dip, dport, proto)

    def delete_log(self, pico_id, portmaps=None):
        # switching to nfqueue
        # self._log_func(pico_id, iptables.delete_log, self.local)
        if not portmaps:
            portmaps = self._get_portmaps(pico_id)
        for portmap in portmaps:
            dip, dport, proto = (portmap.dip, portmap.dport, portmap.proto)
            self.cfqueue.release(dip, dport, proto)
            iptables.delete_queue(dip, dport, proto)


################
## The Worker ##
################
class Worker:

    def __init__(self, config):
        self.config = config
        self.has_started = False
        self.is_stopping = False
        self.picoman = PicoManager(config, self.pico_exit_callback)
        self.portmapper = PortMapper(self)
        self.resourceman = ResourceManager(self)
        self.hub = None
        self.find_public_ips()
        self.timing = {}
        self.loop = None

    def find_public_ips(self):
        # TODO: kind of hacky, find a local way of doing this
        self.heart_ip = urllib2.urlopen("http://ipecho.net/plain").read()
        # TODO: find all other IPs managed by this worker and include in self.public_ips
        self.public_ips = [self.heart_ip]

    def set_hub(self, hub):
        self.hub = hub

    def pico_exit_callback(self, pico):
        pico_id = pico.pico_id
        self.picoman.release(pico_id)
        self.resourceman.remove(pico_id)

    def port_callback(self, dip, dport, proto):
        logger.info("port_callback")
        port_key = iptables.get_port_key(dip, dport, proto)
        pico_id = self.portmapper.find(port_key)
        if pico_id:
            pico = self.picoman.get_pico_by_id(pico_id)
            if pico.status == Pico.CHECKPOINTED:
                self.pico_ensure_alive(pico_id)
            else:
                logger.debug("pico_ensure_alive(%d): this pico is in state %d. must be in state %d to restore from warm.",pico_id,pico.status, Pico.CHECKPOINTED)

    def _process_stop_listener(self, p):
        if p == self.coordinator:
            logger.critical('Coordinator has stopped.')
            self.stop()
        elif p == self.zftp:
            logger.critical('Zftp has stopped.')
            # should we restart it?
        if self.coordinator.stopped and self.zftp.stopped:
            exit()

    def _run_in_new_thread(self, func, args):
        logger.info("worker._run_in_new_thread, func=%s",func.__name__)
        thread = threading.Thread(target=func, args=args)
        thread.start()
        return thread

    def _setup_portmapper(self, pico_id, portmaps, hub_callback):
        self.portmapper.add_pico(pico_id,portmaps)
        self.portmapper.install_log(pico_id,portmaps)
        if hub_callback and 'func' in hub_callback:
            hub_callback['func'](0,hub_callback['addr'])

    def pico_exec(self, pico_id, internal_ip, s_portmaps, exec_count, hub_callback=None):
        ret = 0
        self.timing['pico_exec'] = datetime.now()
        logger.info('pico_exec(%s, %s, %s, %d)',
            pico_id, internal_ip, s_portmaps, exec_count)
        portmaps = iptables.convert_portmaps(s_portmaps)
        self.portmapper.add_pico(pico_id,portmaps)
        self.portmapper.install_log(pico_id,portmaps)
        if hub_callback and 'func' in hub_callback:
            hub_callback['func'](0,hub_callback['addr'])
        # thread = self._run_in_new_thread(self._setup_portmapper, [pico_id, portmaps, hub_callback])
        self.picoman.execute(pico_id, internal_ip, portmaps, exec_count)
        # thread.join()

        self.portmapper.delete_log(pico_id,portmaps)
        self.resourceman.make_hot(pico_id)
        logger.debug('pico_exec finished')
        return ret

    def quick_exec(self, pico_id, exec_count, portmap={80:10080,443:10443}):
        internal_ip = '192.168.122.' + str(pico_id)
        s_portmap = ""
        print portmap
        for private_port in portmap:
            public_port = portmap[private_port]
            s_portmap+="{0}:{1}.{2}={3}:{4};".format(self.config.eth0addr, public_port, "TCP", internal_ip, private_port)
            s_portmap+="{0}:{1}.{2}={3}:{4};".format(self.config.eth0addr, public_port, "UDP", internal_ip, private_port)
        s_portmap = s_portmap[:-1]
        self.pico_exec(pico_id, internal_ip, s_portmap, exec_count)

    def pico_gettimes(self, pico_id, state):
        pico = self.picoman.get_pico_by_id(pico_id)
        if state == "warm,hot":
            restore = time_diff(pico.timing['restore_start'],pico.timing['restore_stop'])
            full = time_diff(self.timing['pico_ensure_alive'],pico.timing['restore_stop'])
            return "{0},{1}".format(restore, full)
        elif state == "cold,hot":
            dl_ckpt = time_diff(pico.timing['download_checkpoint_start'], pico.timing['download_checkpoint_stop'])
            read_as = time_diff(pico.timing['read_as_start'],pico.timing['read_as_stop'])
            prefetch = time_diff(pico.timing['prefetch_start'],pico.timing['prefetch_stop'])
            btrfs = time_diff(pico.timing['btrfs_recieve_start'],pico.timing['bind_mount_stop'])
            restore = time_diff(pico.timing['restore_start'],pico.timing['restore_stop'])
            full = time_diff(self.timing['pico_exec'],pico.timing['restore_stop'])
            return "{0},{1},{2},{3},{4},{5}".format(dl_ckpt, read_as, prefetch, btrfs, restore, full)
        else:
            return 'cant understand {0}'.format(state)

    def pico_getsizes(self, pico_id):
        ''' Returns downloaded files sizes after the pico is init and restore. 

        Note: this function only return meaningful results if the pico is restore
        from cold, and profile_downloads flag is on.
        '''
        pico = self.picoman.get_pico_by_id(pico_id)
        sz_init = pico.profiling.get('download_init', -1)
        sz_restore = pico.profiling.get('download_restore', -1)
        return "{0},{1}".format(sz_init, sz_restore)

    def pico_getsize(self, pico_id):
        pico = self.picoman.get_pico_by_id(pico_id)
        return pico.get_download_size()

    def pico_release(self, pico_id, clean=True):
        self.timing['pico_release'] = datetime.now()
        logger.info('pico_release(%s)', pico_id)
        pico = self.picoman.get_pico_by_id(pico_id)
        self.portmapper.remove_pico(pico_id)
        self.resourceman.remove(pico_id)
        exec_count = self.picoman.release(pico_id, clean)
        self.hub.pico_release(pico_id, exec_count)
        if self.config.profile_uploads:
            return pico.profiling['upload_ckpt']

    def pico_nap(self, pico_id):
        self.timing['pico_nap'] = datetime.now()
        def ckpt_callback(pico):
            logger.info("pico_nap checkpoint callback")
            pass
        self.portmapper.install_log(pico_id)
        self.picoman.checkpoint(pico_id, ckpt_callback)
        self.resourceman.make_warm(pico_id)

    def pico_kill(self, pico_id):
        logger.info('pico_kill(%s)', pico_id)
        self.portmapper.remove_pico(pico_id)
        self.picoman.kill(pico_id)
        self.resourceman.remove(pico_id)

    def pico_ensure_alive(self, pico_id):
        self.timing['pico_ensure_alive'] = datetime.now()
        logger.info('pico_ensure_alive(%s)', pico_id)
        self.picoman.ensure_alive(pico_id)
        self.portmapper.delete_log(pico_id)
        self.resourceman.make_hot(pico_id)

    def start(self):
        if self.has_started:
            return
        config = self.config
        ensure_dir_exists(config.worker_var_dir)
        ProcessBase.add_process_stop_listener(self._process_stop_listener)

        self.has_started = True
        # self._setup_sigchld_handler()

    def _kill_processes(self, processes):
        for p in processes:
            p.kill()

    def cleanup(self, pico):
        self.portmapper.delete_dnat(pico)
        self.portmapper.delete_log(pico)
        self.picoman.clear(pico)
        self.picoman._delete(pico)

    def stop(self):
        # TODO DO WE REALLY WANT TO DO THIS? ONLY FOR TESTING PROBABLY...
        for pico in self.picoman._picos:
            self.portmapper.delete_dnat(pico)
            self.portmapper.delete_log(pico)
            self.picoman.clear(pico)
        if self.is_stopping:
            return
        self.is_stopping = True
        sys.exit("goodbye.")


#################
## ResourceMan ##
#################
class ResourceManager:
    def __init__(self, worker):
        self.worker = worker
        self.config = worker.config
        self.hot_picos = OrderedDict()
        self.warm_picos = OrderedDict()
        self.status = hubproxy.WorkerStatus.AVAILABLE

    def make_hot(self, pico_id):
        logger.debug('ResourceManager.make_hot(%s)', pico_id)
        if pico_id in self.warm_picos:
            del self.warm_picos[pico_id]
        self.hot_picos[pico_id] = True
        self._should_nap()
        self._update_hub_status()

    def make_warm(self, pico_id):
        logger.debug('ResourceManager.make_warm(%s)', pico_id)
        if pico_id in self.hot_picos:
            del self.hot_picos[pico_id]
        self.warm_picos[pico_id] = True

    def remove(self, pico_id):
        logger.debug('ResourceManager.remove(%s)', pico_id)
        if pico_id in self.hot_picos:
            del self.hot_picos[pico_id]
        if pico_id in self.warm_picos:
            del self.warm_picos[pico_id]

    def _should_nap(self):
        if len(self.hot_picos) > self.config.max_hot_pico_per_worker:
            pico_id = self.hot_picos.popitem(last=False)[0]
            self.worker.pico_nap(pico_id)

    def _update_hub_status(self):
        count = len(self.hot_picos) + len(self.warm_picos)
        status = self.status
        if count > self.config.max_pico_per_worker:
            status = hubproxy.WorkerStatus.OVERLOADED
            if len(self.warm_picos):
                pico_id = self.warm_picos.popitem(last=False)[0]
            else:
                pico_id = self.hot_picos.popitem(last=False)[0]
            self.worker.pico_release(pico_id, clean=True)
        # TODO self.worker.hub.pico_release(EXEC_COUNT)
        elif count < self.config.worker_available_threshold:
            status = hubproxy.WorkerStatus.AVAILABLE
        if self.status != status:
            self.worker.hub.update_worker_status(status)
            logger.info('hub.update_worker_status(%s)', status)
            self.status = status


def main_event_loop():
    _config = config.WorkerConfig()
    _config.load_args()
    logger.debug(_config)
    import zmq, zmq.eventloop
    ioloop = zmq.eventloop.ioloop
    loop = ioloop.ZMQIOLoop()
    loop.install()

    # iptables.dnat('192.168.1.50', 8080, 'tcp', '10.2.0.5', 8080)
    # iptables.log('192.168.1.50', 8080, 'tcp')
    def sigint_handler(sig, frame):
        logger.critical('Receive SIGINT. Stopping...')
        worker.stop()
    signal.signal(signal.SIGINT, sigint_handler)

    import interface

    if not os.path.exists(_config.rsyslog_conf):
        logger.critical('iptables log listener is not ready. run prepare-log-listener.sh')
        sys.exit(-1)
    if not os.path.exists(_config.lxc.fuse_mount_dir):
        logger.critical('Fuse is not running (%s does not exist)',_config.lxc.fuse_mount_dir)
        sys.exit(-1)

    ensure_dir_exists(_config.lxc.fuse_mount_dir)
    ensure_dir_exists(_config.lxc.filesystem_root)
    worker = Worker(_config)
    worker.start()
    worker.loop = loop

    HubConnectionClass = hubproxy.HubConnection
    if _config.local_run:
        HubConnectionClass = hubproxy.MockHubConnection
    hub = HubConnectionClass(loop, worker)
    worker.set_hub(hub)
    hub.connect()
    hub.start()

    # proto must be in captital letters
    # worker.pico_exec(5, '10.2.0.5', '192.168.1.50:4040.TCP=10.2.0.5:8080', False)

    server = interface.TcpEvalServer(worker, io_loop=loop)
    server.listen(1234)
    # log_listener = iptables.IptablesLogListener()
    # log_listener.port_callback = worker.port_callback
    # log_listener.bind(loop)

    loop.start()

if __name__ == '__main__':
    if os.getuid() != 0:
        logger.critical("Worker must be run as root to manage lxc")
        sys.exit(-1)
    main_event_loop()
