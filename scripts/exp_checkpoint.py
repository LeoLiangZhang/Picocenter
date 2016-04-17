#!/usr/bin/env python
'''
To test AS checkpoint, use this
sudo python /elasticity/worker/worker.py --app_name none --profile_downloads true --profile_uploads true

To test page-by-page checkpoint, use these commands to start worker and fuse:
PICO_BLOCK_SIZE=4096 /elasticity/fuse/main /checkpoints/ -f 
sudo python /elasticity/worker/worker.py --app_name none --profile_downloads true --profile_uploads true --block_size 4096 --as_algorithm none
'''
import os
import sys
import time
import subprocess
import urllib2
from datetime import datetime
import argparse
import logging
import time
import socket
import math
import helper


time_diff = helper.time_diff
logger = helper.get_logger('exp_checkpoint')


class WorkerCommander:
    def __init__(self, srv_ip, srv_port):
        self.srv_ip = srv_ip
        self.s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.s.connect((srv_ip, srv_port))

    def send_line(self, text):
        self.s.send(text+'\n')

    def recv_line(self):
        return self.s.recv(1500)

    def invoke_cmd(self, cmd):
        """ Invoke worker command and wait response.
        """
        logger.debug('[WorkerCommander] invoke_cmd ' + repr(cmd)) 
        self.send_line(cmd)
        buf = ''
        while True:
            line = self.recv_line()
            buf += line
            if line.find('\n') >= 0:
                logger.debug('[WorkerCommander] received ' + repr(buf)) 
                return buf


class PicoShell:
    def __init__(self, pico_id):
        self.pico_id = pico_id
        self.pico_ip = '192.168.122.{0}'.format(pico_id)
        self.shell_port = 4444

    def invoke_cmd(self, cmd):
        # Create a new connection to the ncshell, and send cmd with appended exit command.
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect((self.pico_ip, self.shell_port))
        real_cmd = "{0} ; exit ;\n".format(cmd)
        logger.debug('[PicoShell] invoke_cmd ' + repr(cmd)) 
        sock.send(real_cmd)
        result = ''
        while True:
            data = sock.recv(1500)
            if len(data) == 0:
                break
            result += data
        sock.close()
        logger.debug('[PicoShell] received ' + repr(result)) 
        return result


class ExpCheckpointBase(object):
    ''' This is a base class for all checkpoint related experiments. DO NOT use
    this class directly.
    '''
    def __init__(self, pico_id, total_pages, working_pages, offset_pages):
        self.pico_id = pico_id
        self.shell = PicoShell(self.pico_id)
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

        self.total_pages = total_pages
        self.working_pages = working_pages
        self.offset_pages = offset_pages

    def _ping_pico(self, msg='hello', address=None):
        if not address:
            address = ('192.168.122.{0}'.format(self.pico_id), 2345)
        t1 = datetime.now()
        self.sock.sendto(msg, address)
        data, addr = self.sock.recvfrom(1024)
        assert data == msg
        t2 = datetime.now()
        logger.debug('[ping_pico] ping %s bytes, rtt %s', len(msg), time_diff(t1, t2))
        return data, addr

    def _start_pageloop(self):
        cmd = 'start-stop-daemon --start -b -x /opt/toy/pageloop -- {0} {1} {2}'.format(
            self.total_pages, self.working_pages, self.offset_pages)
        self.shell.invoke_cmd(cmd)

    def format_result(self, times, sizes):
        fmt_result = 'exp(total={0},working={1},offset={2}) times: {3} sizes: {4}'.format(
            self.total_pages, self.working_pages, self.offset_pages, 
            ' '.join(map(str, times)), ' '.join(map(str, sizes)))
        return fmt_result


class ExpASCheckpoint(ExpCheckpointBase):
    def __init__(self, wc, pico_id=5, total_pages=256, working_pages=1, offset_pages=0):
        super(ExpASCheckpoint, self).__init__(pico_id, total_pages, working_pages, offset_pages)
        self.exec_count = 0
        self.external_port = 10000+self.pico_id
        self.wc = wc

    def setup(self):
        cmd = 'worker.quick_exec({0}, {1}, {{2345:{2}}})'.format(
            self.pico_id, self.exec_count, self.external_port)
        self.wc.invoke_cmd(cmd)
        time.sleep(1)
        self._start_pageloop()
        cmd = 'worker.pico_nap({0})'.format(self.pico_id)
        self.wc.invoke_cmd(cmd)
        self.exec_count += 1
        cmd = 'worker.pico_ensure_alive({0})'.format(self.pico_id)
        self.wc.invoke_cmd(cmd)
        self._ping_pico()
        time.sleep(1)
        self.release()
        # skip first, the first restore downloads more than the rest
        time.sleep(1)
        self.run_once()
        time.sleep(1)
        self.run_once()
        time.sleep(1)


    def release(self):
        # release can also be used for destroy/remove the pico
        cmd = 'worker.pico_release({0})'.format(self.pico_id)
        upload_ckpt = self.wc.invoke_cmd(cmd)
        self.exec_count += 1
        return int(upload_ckpt)

    def run_once_1(self):
        times = []
        sizes = []
        cmd = 'worker.quick_exec({0}, {1}, {{2345:{2}}})'.format(
            self.pico_id, self.exec_count, self.external_port)
        self.wc.invoke_cmd(cmd)
        
        cmd = 'worker.pico_gettimes({0}, "cold,hot")'.format(self.pico_id)
        result = self.wc.invoke_cmd(cmd)
        logger.debug('[run_once] pico_gettimes '+result)
        dl_ckpt, read_as, prefetch, btrfs, restore, full = map(int, result.split(','))
        times.append(dl_ckpt + read_as)
        times.append(restore)

        cmd = 'worker.pico_getsizes({0})'.format(self.pico_id)
        result = self.wc.invoke_cmd(cmd)
        logger.debug('[run_once] pico_getsizes '+result)
        sz_init, sz_restore = map(int, result.split(','))
        sizes = [sz_init, sz_restore]
        
        t1 = datetime.now()
        self._ping_pico()
        t2 = datetime.now()
        td = time_diff(t1, t2)
        times.append(td)

        cmd = 'worker.pico_getsize({0})'.format(self.pico_id)
        result = self.wc.invoke_cmd(cmd)
        logger.debug('[run_once] pico_getsize '+result)
        sz_running = int(result)
        sizes.append(sz_running)

        # convert to delta sizes
        sizes = map(lambda a: a[0]-a[1], zip(sizes, [0]+sizes[0:-1]))
        return times, sizes

    def run_once(self):
        times, sizes = self.run_once_1()
        upload_ckpt = self.release()
        sizes.append(upload_ckpt)
        return times, sizes


class ExpASMultiple(object):
    def __init__(self, wc, repeat=20):
        self.wc = wc
        self.repeat = repeat

    def run_one_exp(self, pico_id=5, total_pages=256, working_pages=1, offset_pages=0):
        exp = ExpASCheckpoint(self.wc, pico_id=pico_id, total_pages=total_pages, 
            working_pages=working_pages, offset_pages=offset_pages)
        exp.setup()
        for i in range(self.repeat):
            times, sizes = exp.run_once()
            fmt_result = exp.format_result(times, sizes)
            logger.info(fmt_result)
            time.sleep(1)

    def run_simple_exps(self):
        m = 256
        totals = [1*m, 4*m, 16*m, 64*m, 256*m]
        for t in totals:
            self.run_one_exp(t)
            time.sleep(1)

    def run_all_exps(self):
        pico_id = 5
        # number of total pages
        m = 256
        totals = [1*m, 4*m, 16*m, 64*m, 256*m]
        # totals = [64*m]
        # number of working pages
        _4k = 1
        workings = [2**i for i in range(1+int(math.log(_4k*2048, 2)))]
        # workings = [2048]
        offsets = [16384-2**i for i in range(1+int(math.log(_4k*2048, 2)))]
        # offsets = [8, 16, 32, 64, 128, 256, 512, 1024, 2048]
        for t in totals:
            for w in workings:
                if w > t:
                    continue
                for o in offsets:
                    self.run_one_exp(pico_id=pico_id,
                        total_pages=t, working_pages=w, offset_pages=o)
                    time.sleep(1)
                    pico_id +=1

def main():
    wc = WorkerCommander('52.91.124.236', 1234)
    exp = ExpASMultiple(wc)
    # exp.run_all_exps()
    # exp.run_simple_exps()
    # exp = ExpASMultiple(wc, repeat=1)
    exp.run_one_exp(1*256)

if __name__ == '__main__':
    main()
