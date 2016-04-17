''' iptables provides iptables wrapper functions for PicoCenter.
'''

import os, sys
import subprocess
import socket, errno
import re

import config
logger = config.logger

IPTABLES_PATH = config.IPTABLES_HELPER_PATH

def enable_ip_forward():
    with open('/proc/sys/net/ipv4/ip_forward', 'w') as f:
        f.write('1')

##################################################
## WARNING: "-p tcp" MUST set before --dport!!! ##
##################################################
def dnat(dip, dport, proto, tip, tport, local=False):
    # e.g., iptables -t nat -A PREROUTING -d 1.2.3.4 -p tcp --dport 80 \
    #                -j DNAT --to 10.2.0.5:8080
    # local: iptables -t nat -A OUTPUT -o lo -p TCP --dport 8080 -j
    # DNAT --to-destination 192.168.122.111:80
    if local:
        args = [IPTABLES_PATH, '-t', 'nat', '-A', 'OUTPUT',
                '-o', 'lo', '-p', proto, '--dport', dport, '-j', 'DNAT',
                '--to-destination', '%s:%s'%(tip, tport)]
    else:
        args = [IPTABLES_PATH, '-t', 'nat', '-A', 'PREROUTING',
                '-d', dip, '-p', proto, '--dport', dport, '-j', 'DNAT',
                '--to', '%s:%s'%(tip, tport)]
    args = map(str, args)
    rc = subprocess.call(args)
    logger.debug('iptables.dnat %s:%s.%s=%s:%s', dip, dport, proto, tip, tport)
    return rc == 0

def delete_dnat(dip, dport, proto, tip, tport, local=False):
    if local:
        args = [IPTABLES_PATH, '-t', 'nat', '-D', 'OUTPUT',
                '-o', 'lo', '-p', proto, '--dport', dport, '-j', 'DNAT',
                '--to-destination', '%s:%s'%(tip, tport)]
    else:
        args = [IPTABLES_PATH, '-t', 'nat', '-D', 'PREROUTING',
                '-d', dip, '-p', proto, '--dport', dport, '-j', 'DNAT',
                '--to', '%s:%s'%(tip, tport)]
    args = map(str, args)
    rc = subprocess.call(args)
    logger.debug('iptables.delete_dnat %s:%s.%s=%s:%s', dip, dport, proto, tip, tport)
    return rc == 0

def log(dip, dport, proto, local=False):
    # log rule must use INSERT, because if log rule append after the
    # forward rule, then it takes no effect.
    # e.g., iptables -t nat -I PREROUTING -d 1.2.3.4 --dport 80 -p tcp \
    #                -j LOG --log-prefix='[embassies_iptables]'
    if local:
        args = [IPTABLES_PATH, '-t', 'nat', '-I', 'OUTPUT', '-o', 'lo',
                '-d', dip, '-p', proto, '--dport', dport, '-j', 'LOG',
                config.IPTABLES_ARG_LOG_PREFIX]
    else:
        args = [IPTABLES_PATH, '-t', 'nat', '-I', 'PREROUTING',
                '-d', dip, '-p', proto, '--dport', dport, '-j', 'LOG',
                config.IPTABLES_ARG_LOG_PREFIX]
    args = map(str, args)
    rc = subprocess.call(args)
    logger.debug('iptables.log %s:%s.%s', dip, dport, proto)
    return rc == 0

def delete_log(dip, dport, proto, local=False):
    if local:
        args = [IPTABLES_PATH, '-t', 'nat', '-D', 'OUTPUT', '-o', 'lo',
                '-d', dip, '-p', proto, '--dport', dport, '-j', 'LOG',
                config.IPTABLES_ARG_LOG_PREFIX]
    else:
        args = [IPTABLES_PATH, '-t', 'nat', '-D', 'PREROUTING',
                '-d', dip, '-p', proto, '--dport', dport, '-j', 'LOG',
                config.IPTABLES_ARG_LOG_PREFIX]
    args = map(str, args)
    rc = subprocess.call(args)
    logger.debug('iptables.delete_log %s:%s.%s', dip, dport, proto)
    return rc == 0

def install_queue(dip, dport, proto, queue_num=config.WorkerConfig.queue_num):
    ''' Append a jump queue rule to mangle table.

    Ex.
    iptables -t mangle -I PREROUTING -d 192.168.39.60/32 -p tcp -m tcp --dport 10042 -j NFQUEUE --queue-num 1
    '''
    args = [IPTABLES_PATH, '-t', 'mangle', '-I', 'PREROUTING',
            '-d', dip, '-p', proto, '--dport', dport, '-j', 'NFQUEUE', '--queue-num', queue_num]
    args = map(str, args)
    rc = subprocess.call(args)
    logger.debug('iptables.install_queue %s:%s.%s', dip, dport, proto)
    return rc == 0

def delete_queue(dip, dport, proto, queue_num=config.WorkerConfig.queue_num):
    ''' Delete a jump queue rule to mangle table.

    Ex.
    iptables -t mangle -I PREROUTING -d 192.168.39.60/32 -p tcp -m tcp --dport 10042 -j NFQUEUE --queue-num 1
    '''
    args = [IPTABLES_PATH, '-t', 'mangle', '-D', 'PREROUTING',
            '-d', dip, '-p', proto, '--dport', dport, '-j', 'NFQUEUE', '--queue-num', queue_num]
    args = map(str, args)
    rc = subprocess.call(args)
    logger.debug('iptables.delete_queue %s:%s.%s', dip, dport, proto)
    return rc == 0

class IptablesLogListener:
    ''' Async UDP log reciever. Use tonado style ioloop.
    '''
    def __init__(self):
        self.sock = None
        self.loop = None
        self.port_callback = None

    def read_log(self, data):
        # logger.info("read_log")
        lst = data.split()
        dst = ''
        port = ''
        proto = ''
        for item in lst:
            if item.startswith('DST='):
                dst = item[4:]
            elif item.startswith('DPT='):
                port = item[4:]
            elif item.startswith('PROTO='):
                proto = item[6:]
            if dst and port and proto:
                break
        logger.debug('IptablesLogListener received request for (%s:%s.%s).',
                     dst, port, proto)
        if self.port_callback:
            self.port_callback(dst, port, proto)

    def _read_handler(self, fd, events):
        # logger.debug("_read_handler starting")
        while True:
            data = None
            try:
                data, address = self.sock.recvfrom(4096)
                # logger.debug("_read_handler recvd")
                logger.debug('[_read_handler] iptables log data: %s', data)
            except socket.error as e:
                logger.debug("[_read_handler] socket error %s", e)
                if e.args[0] in (errno.EWOULDBLOCK, errno.EAGAIN):
                    # stop current iteration, wait next available
                    return
                raise
            if data:
                self.read_log(data)

    def bind(self, loop, ip='127.0.0.1', port=12345):
        self.loop = loop
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock = sock
        sock.setblocking(False)
        sock.bind((ip, port))
        logger.debug('IptablesLogListener UDP socket binds %s:%s', ip, port)
        loop.add_handler(sock.fileno(), self._read_handler, loop.READ)


def get_port_key(dip, dport, proto):
    return '{0}:{1}.{2}'.format(dip, dport, proto)

class PortMap:
    def __init__(self, dip, dport, proto, tip, tport):
        self.dip = dip
        self.dport = dport
        self.proto = proto
        self.tip = tip
        self.tport = tport
        self.port_key = get_port_key(dip, dport, proto)

re_portmap = re.compile('([\d\.]*):(\d*)\.(\w*)=([\d\.]*):(\d*)')
def convert_portmaps(str_portmaps):
    result = []
    lst = str_portmaps.split(';')
    for item in lst:
        m = re_portmap.match(item)
        if m:
            dip, dport, proto, tip, tport = m.groups()
            portmap = PortMap(dip, dport, proto, tip, tport)
            result.append(portmap)
    return result


def main():
    # install_queue("192.168.39.60", 10042, "TCP")
    pass

if __name__ == '__main__':
    main()

