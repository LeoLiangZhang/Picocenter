#!/usr/bin/env python

import socket, time, os, sys
import subprocess
import urllib2
from datetime import datetime
import argparse, logging
import time
import socket, smtplib
import random

# external lib
# install dnspython via pip: pip install dnspython
# or via apt-get in ubuntu: apt-get install python-dnspython
import dns.resolver

# my libs
import helper

parser = argparse.ArgumentParser(description='run end-to-end test')
parser.add_argument('--test', default='lighttpd', help='possible values are: lighttpd, postfix, bind and clamd')
parser.add_argument('--hub_ip', default='52.3.220.217')
parser.add_argument('--worker_ip', default='52.91.124.236')
parser.add_argument('--worker_port', default=1234, type=int)
# curl should support c-ares, which is need for --dns-servers option
parser.add_argument('--curl_bin_path', default='/Users/liang/.local/curl/bin/curl')
parser.add_argument('--curl_write_out', default='@/elasticity/scripts/curl-format-compact', help='control curl to display info. See curl --write-out')
parser.add_argument('--curl_output', default='curl.out')
parser.add_argument('-c', '--count', default=10, type=int, help='number of times to run the test')
parser.add_argument('--pico_id', default=-1, type=int)
parser.add_argument('--pico_state', default='hot', help='pick expected state of the pico from cold and hot')
parser.add_argument('--wait_time', default=1, type=int, help='sleep $wait_time seconds in each runs of non-hot pico experiments')
parser.add_argument('--set_mail_debug', default=False, action="store_true", help='use this flag to debug send mail')
parser.add_argument('--full_restore', default=False, action="store_true", help='download all pages, restore at once')
args = None #parser.parse_args()

logger = helper.get_logger()


def make_pico_uri(pico_id):
    fmt = '{0:05}.pico'
    return fmt.format(pico_id)


def ping(address):
    count = 5
    ret = subprocess.check_output(['ping', '-c', str(count), address])
    lines = ret.splitlines()
    for i in range(1, 1+count):
        ping_line = 'PING: {0}'.format(lines[i])
        logger.info(ping_line)
    ping_stat_1 = 'PING-STAT-1: '+lines[-2]
    ping_stat_2 = 'PING-STAT-2: '+lines[-1]
    logger.info(ping_stat_1)
    logger.info(ping_stat_2)
    print ping_line
    print ping_stat_1
    print ping_stat_2
    return ping_line


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
        logger.debug('invoke_cmd.send srv_ip=%s %s', self.srv_ip, repr(cmd))
        self.send_line(cmd)
        buf = ''
        while True:
            line = self.recv_line()
            buf += line
            if line.find('\n') >= 0:
                logger.debug('invoke_cmd.recv srv_ip=%s %s', self.srv_ip, repr(buf))
                return buf

def _print_result(test, state, result):
    fmt_result = 'test-{0}-{1}: {2}'
    result_line = fmt_result.format(test, state, result)
    logger.info(result_line)
    print result_line # output result to stdout for further processing
    sys.stdout.flush()

def _run_end2end_cold(pico_id, wc, func):

    result_hot = func()
    time.sleep(args.wait_time)

    # set pico to warm
    cmd = 'worker.pico_nap({0})'.format(pico_id)
    wc.invoke_cmd(cmd)
    time.sleep(args.wait_time)

    result = func()
    logger.debug(result)
    cmd = 'worker.pico_gettimes({0}, "{1},hot")'.format(pico_id, 'warm')
    result += ','+wc.invoke_cmd(cmd).strip()
    result_warm = result

    # set pico to cold
    cmd = 'worker.pico_nap({0})'.format(pico_id)
    wc.invoke_cmd(cmd)
    time.sleep(args.wait_time)
    cmd = 'worker.pico_release({0})'.format(pico_id)
    wc.invoke_cmd(cmd)
    time.sleep(args.wait_time)

    result = func()
    logger.debug(result)
    cmd = 'worker.pico_gettimes({0}, "{1},hot")'.format(pico_id, 'cold')
    result += ','+wc.invoke_cmd(cmd).strip()
    result_cold = result

    _print_result(args.test, 'cold', result_cold)
    _print_result(args.test, 'warm', result_warm)
    _print_result(args.test, 'hot', result_hot)

def run_end2end_cold(pico_id, wc, func):
    retries = 0
    while True:
        func() # make sure alive
        try:
            _run_end2end_cold(pico_id, wc, func)
            break
        except Exception as e:
            logger.warn(e)
        retries += 1
        if retries > 3:
            raise Exception('Fail too many times.')

def run_end2end(func):
    pico_id = args.pico_id
    # prepare pico, make sure it exists
    if args.pico_state != 'hot':
        logger.debug('prepare pico state: %s', args.pico_state)
        wc = WorkerCommander(args.worker_ip, args.worker_port)
        # make sure things have set up
        result = func()
        logger.debug('call func from prepare: %s', result)
        logger.debug('skip first C/R')
        cmd = 'worker.pico_nap({0})'.format(pico_id)
        wc.invoke_cmd(cmd)
        time.sleep(args.wait_time)
        cmd = 'worker.pico_release({0})'.format(pico_id)
        wc.invoke_cmd(cmd)
        time.sleep(args.wait_time)
        result = func()
        logger.debug('Finish preparation, the pico is hot')

    for i in range(args.count):
        if args.pico_state == 'hot':
            result = func()
            logger.debug(result)
            _print_result(args.test, args.pico_state, result)
        # speical sauce for cold pico
        # warm not support yet
        elif args.pico_state == 'cold':
            ping(args.hub_ip)
            run_end2end_cold(pico_id, wc, func)
        else:
            raise Exception('Not support state: '+args.state)

        time.sleep(args.wait_time)

def run_full_end2end(func):
    assert args.pico_state == 'cold'
    wc = WorkerCommander(args.worker_ip, args.worker_port)

    for i in range(args.count):
        pico_id = args.pico_id

        try:
            # make sure things have set up
            result = func()

            # set pico to cold
            cmd = 'worker.pico_nap({0})'.format(pico_id)
            wc.invoke_cmd(cmd)
            time.sleep(args.wait_time)
            cmd = 'worker.pico_release({0})'.format(pico_id)
            wc.invoke_cmd(cmd)
            time.sleep(args.wait_time)

            result = func()
            logger.debug(result)
            cmd = 'worker.pico_gettimes({0}, "{1},hot")'.format(pico_id, 'cold')
            result += ','+wc.invoke_cmd(cmd).strip()
            result_cold = result

            result_hot = func()
            time.sleep(args.wait_time)

            _print_result(args.test, 'cold', result_cold)
            # _print_result(args.test, 'warm', result_warm)
            _print_result(args.test, 'hot', result_hot)
        except Exception as e:
            logger.warn('%s', e)

        args.pico_id += 1


def call_curl(url):
    opts = [args.curl_bin_path, # curl with resolver backend
            '-4', # IPv4
            '-s', # silent
            '-w', args.curl_write_out, # write out, e.g., time_connect
            '--dns-servers', args.hub_ip,
            '-o', args.curl_output, # don't care this too much
            url]
    try:
        logger.debug(' '.join(opts))
        output = subprocess.check_output(opts, stderr=subprocess.STDOUT)
    except subprocess.CalledProcessError as e:
        logger.error('%s curl error: %s', e.returncode, e.output)
        # output = None
        raise e
    return output


def run_lighttpd_test():
    wc = None
    fmt_url = 'http://{0}:{1}/'
    pico_id = args.pico_id
    pico_uri = make_pico_uri(pico_id)
    assert pico_id > 0
    pico_port = 10000 + pico_id
    url = fmt_url.format(pico_uri, pico_port)
    result = call_curl(url) # make sure the pico exists
    return result


def send_mail_to_pico(pico_id, address, port):
    mail_from = 'test@picocenter.org'
    mail_to = 'root@pico{0}.localdomain'.format(pico_id)
    mail_data = 'This is a test message.'
    logger.debug('send_mail_to_pico pico%s %s:%s len(data)=%s',
        pico_id, repr(address), repr(port), len(mail_data))
    server = smtplib.SMTP(address, port)
    if args.set_mail_debug:
        server.set_debuglevel(1)
    server.sendmail(mail_from, mail_to, mail_data)
    server.quit()


def run_postfix_test():
    pico_id = args.pico_id
    pico_port = 10000 + pico_id
    pico_uri = make_pico_uri(pico_id)

    resolver = dns.resolver.Resolver(configure=False)
    resolver.nameservers = [args.hub_ip]
    t1 = datetime.now()
    answers = resolver.query(pico_uri)
    t2 = datetime.now()
    td1 = helper.time_diff(t1, t2)
    if len(answers) == 1:
        a = answers[0]
        logger.debug('resolve %s to %s from %s in %s ms',
            pico_uri, a.address, args.hub_ip, td1)
        t3 = datetime.now()
        send_mail_to_pico(pico_id, a.address, pico_port)
        t4 = datetime.now()
        td2 = helper.time_diff(t3, t4)
        logger.debug('send_mail in %s ms', td2)
        td3 = helper.time_diff(t1, t4)
        # time_dns, time_mail, time_total
        return '{0},{1},{2}'.format(td1, td2, td3)
    elif len(answers) > 1:
        raise Exception('Hub DNS return more than one answer.')
    else:
        raise Exception('No answer return.')


def run_bind_test():
    pico_id = args.pico_id
    pico_port = 10000 + pico_id
    pico_uri = make_pico_uri(pico_id)

    resolver = dns.resolver.Resolver(configure=False)
    resolver.nameservers = [args.hub_ip]
    t1 = datetime.now()
    answers = resolver.query(pico_uri)
    t2 = datetime.now()
    td1 = helper.time_diff(t1, t2)

    if len(answers) != 1:
        raise Exception('answers error, len={0}'.format(len(answer)))

    a = answers[0]
    pico_address = a.address
    logger.debug('resolve %s to %s from %s in %s ms',
            pico_uri, a.address, args.hub_ip, td1)
    # resolver = dns.resolver.Resolver(configure=False)
    # resolver.nameservers = [pico_address]
    # resolver.port = pico_port
    t3 = datetime.now()
    # answers = resolver.query(pico_uri)
    dig_output = subprocess.check_output(['dig',
        '@{0}'.format(pico_address),
        '-p', str(pico_port), 'priv.io'])
    logger.debug('dig: %s', repr(dig_output))
    t4 = datetime.now()
    td2 = helper.time_diff(t3, t4)
    td3 = helper.time_diff(t1, t4)
    return '{0},{1},{2}'.format(td1, td2, td3)


fmt_clamd_conf = '''# copy from alpine_base
LogFile /var/log/clamav/clamd.log
LogTime yes
PidFile /var/run/clamav/clamd.pid
User clamav
AllowSupplementaryGroups yes
TCPAddr {0}
#tcp port, default 3310
TCPSocket {1}
'''

to_be_scan_set = []

def get_file_to_be_scan(root='/usr/bin', count=10, size_limit=1024*1024):
    c = 0
    paths = []
    global to_be_scan_set
    to_be_scan_set_size = 100
    # init to_be_scan_set
    if len(to_be_scan_set) == 0:
        for filename in os.listdir(root):
            if c >= to_be_scan_set_size:
                logger.debug('to_be_scan_set_size: '+repr(to_be_scan_set))
                break;
            path = os.path.join(root, filename)
            s = os.lstat(path)
            stat = os.path.stat
            if not stat.S_ISLNK(s.st_mode) and stat.S_ISREG(s.st_mode) and s.st_size <= size_limit:
                # paths.append(path)
                to_be_scan_set.append(path)
                c += 1
    paths = random.sample(to_be_scan_set,  count)
    return paths

def run_clamd_test():
    pico_id = args.pico_id
    pico_port = 10000 + pico_id
    pico_uri = make_pico_uri(pico_id)

    resolver = dns.resolver.Resolver(configure=False)
    resolver.nameservers = [args.hub_ip]
    t1 = datetime.now()
    answers = resolver.query(pico_uri)
    t2 = datetime.now()
    td1 = helper.time_diff(t1, t2)

    if len(answers) != 1:
        raise Exception('answers error, len={0}'.format(len(answer)))
    a = answers[0]
    pico_address = a.address
    logger.debug('resolve %s to %s from %s in %s ms',
            pico_uri, a.address, args.hub_ip, td1)

    clamd_conf = fmt_clamd_conf.format(pico_address, pico_port)
    with open('clamd.conf', 'wb+') as f:
        f.write(clamd_conf)

    t3 = datetime.now()
    file_to_be_scan = ['clamd.conf']
    file_to_be_scan = get_file_to_be_scan()
    logger.debug('clamdscan files: '+repr(file_to_be_scan))
    subprocess.call(['clamdscan', '-l', 'clamd.log', '--quiet', '--config-file', 'clamd.conf']+ file_to_be_scan)
    t4 = datetime.now()
    td2 = helper.time_diff(t3, t4)
    td3 = helper.time_diff(t1, t4)
    return '{0},{1},{2}'.format(td1, td2, td3)


def main():
    global args
    args = parser.parse_args()
    logger.debug(repr(args))
    if args.full_restore:
        meta_func = run_full_end2end
    else:
        meta_func = run_end2end
    if args.test == 'lighttpd':
        meta_func(run_lighttpd_test)
    elif args.test == 'postfix':
        meta_func(run_postfix_test)
    elif args.test == 'bind':
        meta_func(run_bind_test)
    elif args.test == 'clamd':
        meta_func(run_clamd_test)
    else:
        raise AttributeError('test not support')

if __name__ == '__main__':
    main()

