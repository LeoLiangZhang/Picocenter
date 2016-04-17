#!/usr/bin/env python

import os
import sys
import subprocess
import shutil
import time
from datetime import datetime
import helper
import boto
from boto.s3.key import Key
import exp_checkpoint
from exp_checkpoint import ExpCheckpointBase


time_diff = helper.time_diff
logger = exp_checkpoint.logger

aws_acc = 'Your AWS key'
aws_sec = 'Your AWS sec'


spawn_script = '/elasticity/scripts/clone'
destroy_script = '/elasticity/scripts/destroy'
tmp_root = '/tmp/pico.full/'
if not os.path.exists(tmp_root):
    os.makedirs(tmp_root, 0755)

class FullCheckpointPico:

    def __init__(self, conn, pico_id=105):
        self.pico_id = pico_id
        self.app_name = 'none'
        self.bucket = 'picocenter'
        self.conn = conn  # amazon s3 connection
        self.prefix = 'pico.full/{0}/ckpt.tar.gz'.format(pico_id)  # amazon s3 key for the pico
        self.profiling = {}  # to save profiling vars

        # /tmp/pico.full/<pico_id>/{ckpt/, ckpt.tar.gz}
        self.ckpt_root = os.path.join(tmp_root, str(pico_id))
        self.ckpt_dir = os.path.join(self.ckpt_root, 'ckpt')
        self.ckpt_tar = os.path.join(self.ckpt_root, 'ckpt.tar.gz')
        if not os.path.exists(self.ckpt_root):
            os.makedirs(self.ckpt_root, 0755)

        self.container_name = "pico"+str(pico_id)
        lxcpath = '/usr/local/var/lib/lxc/'
        base_name = 'alpine_base'
        base_path = os.path.join(lxcpath, base_name)
        pico_path = os.path.join(lxcpath, 'pico{pico_id}')
        pico_path = pico_path.format(pico_id=pico_id)
        self.rootfs_path = os.path.join(pico_path, 'rootfs')

    def setup_container(self):
        logger.debug('[setup_container] %s', self.pico_id)
        path_to_user_init = "/bin/ash"
        user_args = "/root/init-{0}.sh".format(self.app_name)
        user_init_cmd = (path_to_user_init + " " + user_args).strip().split(" ")

        ret = subprocess.call([spawn_script, str(self.pico_id)])
        shutil.copy('/elasticity/scripts/init-{0}.sh'.format(self.app_name), 
            os.path.join(self.rootfs_path,'root'))
        shutil.copy('/elasticity/experiment/swap/1k', 
            os.path.join(self.rootfs_path,'var', 'www', 'localhost', 'htdocs', 'index.html'))
        ret = subprocess.call([
                "lxc-attach",
                "-n",
                self.container_name,
                "--"
            ] + user_init_cmd)

    def destroy(self):
        ret = subprocess.call([destroy_script, str(self.pico_id)])
        shutil.rmtree(self.ckpt_root)

    def pico_getsize(self):
        raw_sz = 0
        for name in os.listdir(self.ckpt_dir):
            p = os.path.join(self.ckpt_dir, name)
            s = os.stat(p)
            raw_sz += s.st_size
        return raw_sz

    def checkpoint(self):
        self._clean_ckpt_dir()
        ret = subprocess.call(['lxc-checkpoint', '-s', '-n', 
            self.container_name, '-D', self.ckpt_dir])

    def restore(self):
        t1 = datetime.now()
        ret = subprocess.call(['lxc-checkpoint', '-r', '-n', 
            self.container_name, '-D', self.ckpt_dir])
        t2 = datetime.now()
        td = time_diff(t1, t2)
        self.profiling['time_restore'] = td
        self.profiling['download_restore'] = 0
        logger.debug('[restore] time_restore=%s download_restore=%s', td, 0)

    def _clean_ckpt_dir(self):
        if not os.path.exists(self.ckpt_dir):
            os.makedirs(self.ckpt_dir, 0755)
        else:
            shutil.rmtree(self.ckpt_dir)
            os.makedirs(self.ckpt_dir, 0755)

    def download(self):
        t1 = datetime.now()
        b = self.conn.get_bucket(self.bucket)
        k = b.get_key(self.prefix)
        k.get_contents_to_filename(self.ckpt_tar)
        cwd = os.getcwd()
        os.chdir(self.ckpt_root)
        ret = subprocess.call(['tar', '-xf', 'ckpt.tar.gz'])
        os.chdir(cwd)
        t2 = datetime.now()
        td = time_diff(t1, t2)
        self.profiling['time_init'] = td
        sz = self.pico_getsize()
        self.profiling['download_init'] = sz
        logger.debug('[download] time_init=%s download_init=%s', td, sz)

    def upload(self):
        cwd = os.getcwd()
        os.chdir(self.ckpt_root)
        tar_path = shutil.make_archive('ckpt', 'gztar', '.', 'ckpt')
        logger.debug('[upload] make tar ball %s', tar_path)
        b = self.conn.get_bucket(self.bucket)
        # k = b.get_key(self.prefix)
        k = Key(b)
        k.key = self.prefix
        k.set_contents_from_filename(self.ckpt_tar)
        os.chdir(cwd)
        sz = self.pico_getsize()
        self.profiling['upload_ckpt'] = sz
        
        # print debugging info
        s = os.stat(self.ckpt_tar)
        logger.debug('[upload] %s %s %s', sz, s.st_size, self.ckpt_tar)

        self._clean_ckpt_dir()
        os.remove(self.ckpt_tar)


class ExpFullCheckpoint(ExpCheckpointBase):
    def __init__(self, conn, pico_id=105, total_pages=256, working_pages=1, offset_pages=0):
        super(ExpFullCheckpoint, self).__init__(pico_id, total_pages, working_pages, offset_pages)
        self.conn = conn
        self.pico = FullCheckpointPico(self.conn, self.pico_id)

    def setup(self):
        self.pico.setup_container()
        time.sleep(1)
        self._start_pageloop()
        time.sleep(1)
        self._ping_pico()
        self.pico.checkpoint()
        self.pico.upload()
        time.sleep(1)
        self.run_once()
        time.sleep(1)

    def run_once_1(self):
        self.pico.download()
        self.pico.restore()
        t1 = datetime.now()
        self._ping_pico()
        t2 = datetime.now()
        td = time_diff(t1, t2)
        self.pico.profiling['time_running'] = td
        self.pico.profiling['download_running'] = 0

    def run_once(self):
        self.run_once_1()
        self.pico.checkpoint()
        self.pico.upload()
        p = self.pico.profiling
        times = [p['time_init'], p['time_restore'], p['time_running']]
        sizes = [p['download_init'], p['download_restore'],
                 p['download_running'], p['upload_ckpt']]
        return times, sizes


class ExpFullMultiple(object):
    def __init__(self, conn, repeat=20):
        self.conn = conn
        self.repeat = repeat

    def run_one_exp(self, total_pages=256, working_pages=1, offset_pages=0):
        exp = ExpFullCheckpoint(self.conn, total_pages=total_pages,
            working_pages=working_pages, offset_pages=offset_pages)
        exp.setup()
        for i in range(self.repeat):
            times, sizes = exp.run_once()
            fmt_result = exp.format_result(times, sizes)
            logger.info(fmt_result)
            time.sleep(1)
        exp.pico.destroy()

    def run_exps(self):
        m = 256
        totals = [1*m, 4*m, 16*m, 64*m, 256*m]
        totals = [256*m]
        w = 2048
        for t in totals:
            self.run_one_exp(total_pages=t, working_pages=w)
            time.sleep(1)

    def run_all_exps(self):
        m = 256
        # number of total pages
        totals = [1*m, 4*m, 16*m, 64*m, 256*m]
        _4k = 1
        # number of working pages
        workings = [2**i for i in range(1+int(math.log(_4k*2048, 2)))]
        for t in totals:
            for w in workings:
                if w > t:
                    continue
                self.run_one_exp(total_pages=t, working_pages=w)
                time.sleep(1)


def main():
    conn = boto.connect_s3(aws_acc, aws_sec)
    exp = ExpFullMultiple(conn)
    exp.run_exps()
    

if __name__ == '__main__':
    main()
