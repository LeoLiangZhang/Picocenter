import zmq
import sys
from PicoManager import PicoManager
from random import choice
from time import sleep
from threading import Thread
from multiprocessing import Process
import msgpack
import MySQLdb
import signal

PICOPROCESS_TABLE = 'picos'
WORKER_TABLE = 'workers'
ADDR_TABLE = 'addrmap'
META_TABLE = 'meta'

import config
logger = config.logger

class MessageType:
    PICO_RELEASE = 'R'
    PICO_KILL = 'K'
    UPDATE_STATUS = 'S'
    HELLO = 'H'
    HEARTBEAT = 'B'
    REPLY = 'Y'
    PICO_EXEC = 'E'
    DEAD = 'D'

class WorkerStatus:
    AVAILABLE = 1
    OVERLOADED = 2

################################################################################

def db_manager_process():

    db = MySQLdb.connect(host='localhost',user='root',passwd='picoparty',db='picocenter')
    db.autocommit(True)
    cursor = db.cursor()

    context = zmq.Context.instance()
    router = context.socket(zmq.ROUTER)
    router.bind("ipc://db.ipc")

    picomanager = PicoManager(db, router)

    while True:

        msg = router.recv()
        # print msg
        mtype = msg[0]

        to_log = None
        query = None

        if mtype == MessageType.PICO_RELEASE:
            pico_id, exec_count = msgpack.unpackb(msg[1:])
            to_log = "released pico {0} with exec_count={1}".format(pico_id,exec_count)
            query = ("UPDATE {0} SET hot=FALSE,exec_count={2} WHERE pico_id={1}".format(PICOPROCESS_TABLE, pico_id,exec_count))
            # query = ("UPDATE {0} SET hot=FALSE, worker_id=NULL, public_ip=NULL WHERE pico_id={1} ".format(PICOPROCESS_TABLE, pico_id))
        elif mtype == MessageType.PICO_KILL:
            pico_id = msg[1:]
            to_log = "killed pico {0}".format(pico_id)
            query = ("DELETE FROM {0} WHERE pico_id={1}".format(PICOPROCESS_TABLE, pico_id))
        elif mtype == MessageType.UPDATE_STATUS:
            status = msgpack.unpackb(msg[1])
            if status and int(status) is WorkerStatus.AVAILABLE:
                new_status = 1
            else:
                new_status = 0
            worker_ip = msg[2:]
            to_log = "updated status of {0} to {1}".format(worker_ip, new_status)
            query = ("UPDATE {0} SET status={1} WHERE heart_ip='{2}'".format(WORKER_TABLE, new_status, worker_ip))
        elif mtype == MessageType.DEAD:
            worker_ip = msg[1:]
            logger.debug("Worker @ {0} died".format(worker_ip))
            # liang: TODO: fix this later
            # picomanager.migrate_picoprocesses(worker_ip)
        elif mtype == MessageType.HELLO:
            hello = msgpack.unpackb(msg[1:])

            if hello['status'] is WorkerStatus.AVAILABLE:
                status = "available"
            else: # just in case, default to overloaded
                status = "overloaded"

            logger.debug("Found new {0} worker @ {1}, managing: {2}".format(status, hello['heart_ip'], hello['public_ips']))

            query = "INSERT INTO workers SET status='{0}', heart_ip='{1}';".format(hello['status'], hello['heart_ip'])
            cursor.execute(query)

            query = "INSERT INTO ips (worker_id, ip) VALUES"
            for ip in hello['public_ips']:
                query += "(LAST_INSERT_ID(), '{0}'),".format(ip)
            query = query[:-1]

        if query:
            cursor.execute(query)

        if to_log:
            logger.debug(to_log)

    cursor.close()

################################################################################

heartbeats = {}
heartbeat_timeout = 30.0

def monitor_heartbeats():
    monitor_socket = zmq.Context().socket(zmq.DEALER)
    monitor_socket.connect('ipc://db.ipc')
    while True:
        kill = None
        #logger.debug("heartbeat monitor: (workers={0})".format(str(heartbeats)))
        for worker in heartbeats:
            if heartbeats[worker] == 0:
                monitor_socket.send(MessageType.DEAD + worker)
                kill = worker
            else:
                heartbeats[worker] = 0
        if kill:
            del heartbeats[kill]
        sleep(heartbeat_timeout)

################################################################################

args = ['port']
if len(sys.argv) < len(args)+1:
    sys.exit('usage: python hub-heart.py ' + ' '.join(['['+arg+']' for arg in args]))

db_manager = Process(target=db_manager_process)
db_manager.daemon = True
db_manager.start()

monitor = Thread(target=monitor_heartbeats)
monitor.daemon = True
monitor.start()

def sigint_handler(sig, frame):
    logger.critical('Receive SIGINT. Stopping...')
    #db = MySQLdb.connect(host='localhost',user='root',passwd='picoparty',db='picocenter')
    #db.autocommit(True)
    #cursor = db.cursor()
    #cursor.execute("TRUNCATE ips")
    #cursor.execute("TRUNCATE workers")
    #cursor.execute("UPDATE picos SET hot=0")
    #cursor.execute("TRUNCATE picos")

# signal.signal(signal.SIGINT, sigint_handler)

################################################################################

context = zmq.Context.instance()
frontend = context.socket(zmq.ROUTER)
frontend.bind("ipc://frontend.ipc")
backend = context.socket(zmq.ROUTER)
backend.bind("tcp://0.0.0.0:" + sys.argv[1])
dispatcher = context.socket(zmq.DEALER)
dispatcher.connect('ipc://db.ipc')

poller = zmq.Poller()
poller.register(backend, zmq.POLLIN)
workers = False

while True:
    sockets = dict(poller.poll())

    ###########################################################################

    if backend in sockets:

        request = backend.recv_multipart()

        worker, msg = request[:2]
        mtype = msg[0]

        if mtype == MessageType.HELLO:
            if not workers:
                logger.debug("registering frontend")
                poller.register(frontend, zmq.POLLIN)
            worker_ip = worker.split('/')[0]
            workers = True
            heartbeats[worker_ip] = 1
            dispatcher.send(msg)
        elif mtype == MessageType.HEARTBEAT:
            worker_ip = worker.split('/')[0]
            heartbeats[worker_ip] = 1
        elif mtype == MessageType.PICO_RELEASE or mtype == MessageType.PICO_KILL:
            dispatcher.send(msg)
        elif mtype == MessageType.UPDATE_STATUS:
            worker_ip = worker.split('/')[0]
            dispatcher.send(msg + worker_ip)
        elif mtype == MessageType.REPLY:
            dest = request[2]
            logger.debug("[{0} -> {1}] forwarding".format(worker, dest))
            frontend.send_multipart([dest, msg[1:]])
        else:
            logger.critical("unknown message type:" + str(mtype))

    ###########################################################################

    if frontend in sockets:

        logger.debug("frontend")

        resolver, msg = frontend.recv_multipart()
        worker, request = msg.split('|')[0:2] # TODO: [0:2] is a temporary fix..
        worker = worker + "/jobs"

        logger.debug("[{0} -> {1}]: {2}".format(resolver, worker, msg))

        backend.send_multipart([worker, "", resolver, "", MessageType.PICO_EXEC + request])

        if not workers:
            logger.debug("unregistering frontend")
            poller.unregister(frontend)

    ###########################################################################

db_manager.terminate()
backend.close()
frontend.close()
context.term()
