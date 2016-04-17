from twisted.internet import reactor, defer
from twisted.names import client, dns, error, server
import MySQLdb
import sys, os
import random
import zmq
import msgpack
from PicoManager import PicoManager
from time import sleep
import signal

import config
logger = config.logger

################################################################################

RECORD_TTL = 0
PICOPROCESS_TABLE = 'picos'
PICO_FIELDS = "pico_id,exec_count,hot,worker_id,public_ip,internal_ip,ports,hostname,customer_id"

################################################################################

class Picoprocess(object):
    def __init__(self, fields):
        self.pico_id, self.exec_count, self.hot, self.worker_id, self.public_ip, self.internal_ip, self.ports, self.hostname, self.customer_id = fields

class Worker(object):
    def __init__(self, fields):
        self.worker_id, self.status, self.heart_ip, self.heart_port = fields

class AddrMap(object):
    def __init__(self, fields):
        self.worker_id, self.public_ip, self.ports_allocated, self.port_80_allocated = fields

################################################################################

class HubResolver(object):

    def resolve_hostname(self, hostname):

        query = ("SELECT %s FROM %s WHERE hostname='%s'" %
            (PICO_FIELDS, PICOPROCESS_TABLE, hostname))
        cursor = self.db.cursor()
        cursor.execute(query)
        result = cursor.fetchone()

        if result:
            pico = Picoprocess(result)
            if pico.worker_id and pico.public_ip:
                logger.info("pico already assigned to worker {0}".format(pico.public_ip))
                if pico.hot:
                    logger.info("pico is hot!")
                    return pico.public_ip
                else:
                    logger.info("pico is not hot, but has a worker")
                    self.pico_manager._run_picoprocess(pico)
                    return pico.public_ip
            else:
                logger.info("pico not currently assigned to a worker")
                self.pico_manager._run_picoprocess(pico)
                logger.info("pico {0} now running on {1} (internal={2})".format(pico.pico_id, pico.public_ip, pico.internal_ip))
                return pico.public_ip

        logger.debug("could not find {0} in our database...".format(hostname))
        return None

    def query(self, query, timeout=None):
        """
        Lookup the hostname in our database. If we manage it, make sure it's running,
        then return its public IP address, otherwise fail.
        """

        logger.debug("recieved query: {0}".format(str(query)))

        if query.type == dns.A:
            name = query.name.name
            ip = self.resolve_hostname(name)
            logger.debug("hostname resolved")
            if ip:
                answer = dns.RRHeader(
                    name=name,
                    payload=dns.Record_A(address=ip),
                    ttl=RECORD_TTL)
                answers = [answer]
                return defer.succeed((answers,[],[]))
        return defer.fail(error.DomainError())

    def lookupAllRecords(self, name, timeout=None):
        # TODO
        logger.debug("dns: lookupallrecords({0},{1})".format(name,timeout))
        return None

    def __init__(self, dbpasswd, id):

        self.db = MySQLdb.connect(host='localhost',user='root',passwd=dbpasswd,db='picocenter')
        self.db.autocommit(True)

        socket = zmq.Context().socket(zmq.DEALER)
        socket.identity = 'dns-' + str(id).zfill(2)
        socket.connect('ipc://frontend.ipc')
        self.socket = socket

        self.pico_manager = PicoManager(self.db, self.socket)

################################################################################

def main(args):
    """
    Run the server.
    """

    # Need to handle sigchld to protect zmq poll from interrupted system calls
    child_terminated = False
    def handle_sigchld(sig, frame):
        global child_terminated
        child_terminated = True
    signal.signal(signal.SIGCHLD, handle_sigchld)

    mysql_pwd = "picoparty"
    dns_id = "1"
    factory = server.DNSServerFactory(clients=[HubResolver(mysql_pwd,dns_id),client.Resolver(resolv='/etc/resolv.conf')])

    protocol = dns.DNSDatagramProtocol(controller=factory)

    reactor.listenUDP(int(args[1]), protocol)
    reactor.listenTCP(int(args[1]), factory)

    reactor.run()

if __name__ == '__main__':
    args = ['port']
    if len(sys.argv) < len(args)+1:
        sys.exit('usage: python hub-dns.py ' + ' '.join(['['+arg+']' for arg in args]))
    elif sys.argv[1] == '53' and os.getuid() != 0:
        sys.exit('You need to run me as root to bind to port 53!')
    else:
        raise SystemExit(main(sys.argv))
