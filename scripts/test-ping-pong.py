#!/usr/bin/env python

import socket, time, os, sys
import subprocess
import urllib2
from datetime import datetime

srv_ip_1 = '54.152.129.245'
srv_ip_2 = '52.23.178.87' # set to empty string if only one worker
srv_ip_2 = ''
srv_port = 1234
pico_id = 84
count = 2 # run 0 is to create the pico

if len(sys.argv) < 3:
    print "usage:", sys.argv[0], "count", "srv_ip_1", "[srv_ip_2]", "[pico_id]"
    exit(1)
else:
    count = int(sys.argv[1]) if len(sys.argv) >= 2 else count
    srv_ip_1 = sys.argv[2] if len(sys.argv) >= 3 else srv_ip_1
    srv_ip_2 = sys.argv[3] if len(sys.argv) >= 4 else srv_ip_2
    pico_id = int(sys.argv[4]) if len(sys.argv) >= 5 else pico_id

print "srv_ip_1", srv_ip_1
print "srv_ip_2", srv_ip_2
print "count   ", count
print "pico_id ", pico_id

def time_diff(t1, t2):
    delta = t2-t1
    return ((delta.seconds * 1000) + (delta.microseconds / 1000))

class WorkerProxy:

    def __init__(self, srv_ip, srv_port):
        self.srv_ip = srv_ip
        self.s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.s.connect((srv_ip, srv_port))

    def send_line(self, text):
        self.s.send(text+'\n')

    def recv_line(self):
        return self.s.recv(1500)

    def send_cmd(self, cmd):
        print self.srv_ip, cmd
        self.send_line(cmd)
        while True:
            line = self.recv_line()
            print self.srv_ip, line
            if line.find('\n') >= 0:
                break


exec_count = 0
wk1 = WorkerProxy(srv_ip_1, srv_port)
if srv_ip_2:
    wk2 = WorkerProxy(srv_ip_2, srv_port)
else:
    wk2 = wk1
wks = [wk1, wk2]

while exec_count < count:
    wk = wks[exec_count % len(wks)]
    cmd = 'worker.pico_exec({0}, {1})'.format(pico_id, exec_count)
    wk.send_cmd(cmd)
    if exec_count != 0:
        cmd = 'worker.pico_gettimes({0}, "cold,hot")'.format(pico_id)
        wk.send_cmd(cmd)

    url = "http://{0}:{1}/".format(wk.srv_ip, 8081)
    print 'fetch', url,
    t1 = datetime.now()
    # response = urllib2.urlopen(url)
    # html = response.read()
    # sz = len(html)
    ret = subprocess.call(['curl', '-s', '-w', '@/elasticity/Picocenter/scripts/curl-format', '-o', 'curl.out', url])
    t2 = datetime.now()
    sz =os.stat('curl.out').st_size
    print ' => size(body) =', sz, ' in ', time_diff(t1, t2), ' ms ', ret

    cmd = 'worker.pico_nap({0})'.format(pico_id)
    wk.send_cmd(cmd)
    cmd = 'worker.pico_release({0})'.format(pico_id)
    wk.send_cmd(cmd)

    exec_count += 1
