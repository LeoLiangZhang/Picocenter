import zmq
import tornado.ioloop
import zmq.eventloop
from zmq.eventloop.zmqstream import ZMQStream
import msgpack
import urllib2
import random

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

class MockHubConnection:
    def __init__(self, *args, **kwargs):
        pass
    def connect(self, *args, **kwargs):
        pass
    def start(self, *args, **kwargs):
        pass
    def update_worker_status(self, *args, **kwargs):
        pass
    def pico_release(self, *args, **kwargs):
        pass

class HubConnection(object):

    defaults = {
        'heartbeat_interval' : 3000.0
    }

    def poll_hub(self, msg):
        """
        Parses args out of any incoming message,
        then passes these to the the right worker function.
        """

        empty, address, empty, request = msg

        mtype = request[0]
        """
        [MTYPE(1-byte)][MSG]
        """
        #args = request[1:].split(" ")
        args = msgpack.unpackb(request[1:])

        ret = 0
        if mtype == MessageType.PICO_EXEC:
            pico_id, internal_ip, portmap, exec_count = args
            s_portmap = ""
            print portmap
            for private_port in portmap:
                public_port = portmap[private_port]
                s_portmap+="{0}:{1}.{2}={3}:{4};".format(self.worker.config.eth0addr, public_port, "TCP", internal_ip, private_port)
                s_portmap+="{0}:{1}.{2}={3}:{4};".format(self.worker.config.eth0addr, public_port, "UDP", internal_ip, private_port)
            s_portmap = s_portmap[:-1]
            logger.debug("[HUB] -> worker :: pico_exec({0},{1},{2},{3})".format(pico_id, internal_ip, s_portmap, exec_count))
            ret = self.worker.pico_exec(pico_id, internal_ip, s_portmap, exec_count,
                    {
                        'func': self._reply,
                        'addr': address
                    })
        elif mtype == MessageType.PICO_RELEASE:
            logger.debug("[HUB] -> worker :: pico_release({0})".format(args))
            ret = self.worker.pico_release(*args)
        elif mtype == MessageType.PICO_KILL:
            logger.debug("[HUB] -> worker :: pico_kill({0})".format(args))
            ret = self.worker.pico_kill(*args)
        else:
            raise AttributeError("Unknown command type: {0}".format(mtype))

        if mtype != MessageType.PICO_EXEC: # we already replied to the hub via _reply callback
            reply = MessageType.REPLY + str(ret)
            self.job_stream.send_multipart([reply, address])

    def _reply(self,msg,addr):
        logger.debug("Reply to hub that pico is ready...")
        #self.job_stream.send_multipart([MessageType.REPLY+str(msg),addr])

    def heartbeat(self):
        """
        Sends out a heartbeat to the hub.
        Note: The function is called by the heartbeat_timer.
        """
        beat = MessageType.HEARTBEAT
        self.heart_stream.send(beat)

    def set_worker(self, worker):
        self.worker = worker

    def update_worker_status(self, status):
        """
        Changes worker's status in the hub database

        Parameters:
            status : WorkerStatus
        """

        logger.debug("[WORKER] -> hub :: update status from {0} to {1}".format(self.worker_status, status))

        msg = MessageType.UPDATE_STATUS + str(status)
        self.send_stream.send(msg)
        self.worker_status = status

    def pico_release(self, pico_id, exec_count):

        logger.debug("[WORKER] -> hub :: pico_release({0})".format(pico_id))

        args = (pico_id, exec_count)
        msg = MessageType.PICO_RELEASE + msgpack.packb(args)
        self.send_stream.send(msg)

    def pico_kill(self, pico_id):

        logger.debug("[WORKER] -> hub :: pico_kill({0})".format(pico_id))

        msg = MessageType.PICO_KILL + str(pico_id)
        self.send_stream.send(msg)

    def do_handshake(self):
        """
        Used on startup to inform hub of which ips this worker manages, and
        which ip/port combination (haddr) can be used to contact it
        """

        hello = {
            'status' : self.worker_status,
            'heart_ip' : self.worker.heart_ip,
            'public_ips' : self.worker.public_ips
        }

        msg = MessageType.HELLO + msgpack.packb(hello)

        try:
            self.send_socket.send(msg)
        except NameError:
            logger.critical("[WORKER] could not connect to hub: send_socket does not exist")
        except Exception as err:
            logger.critical("[WORKER] could not connect to hub: " + str(err))
            return False

        return True

    def connect(self):
        """
        Creates and starts all necessary threads and sockets for heartbeating,
        job polling, and message passing.

        Returns:
            [0] on success
            [-1] error (not connected to hub)
        """

        logger.debug("[WORKER] hub.connect()")
        endpoint = "tcp://" + self.worker.config.hub_ip + ':' + self.worker.config.hub_port

        context = zmq.Context.instance()

        self.send_socket = context.socket(zmq.DEALER)
        self.send_socket.identity = self.worker.heart_ip + '/send'
        self.send_socket.connect(endpoint)

        self.job_socket = context.socket(zmq.DEALER)
        self.job_socket.identity = self.worker.heart_ip + '/jobs'
        self.job_socket.connect(endpoint)
        self.job_stream = ZMQStream(self.job_socket)
        # self.job_stream.on_recv(self.poll_hub)

        self.heart_socket = context.socket(zmq.DEALER)
        self.heart_socket.identity = self.worker.heart_ip + '/heart'
        self.heart_socket.connect(endpoint)
        self.heart_stream = ZMQStream(self.heart_socket)

        self.heartbeat_timer = tornado.ioloop.PeriodicCallback(self.heartbeat,
            self.heartbeat_interval, self.loop)

        return 0

    def start(self):
        """
        Call this method when the worker is ready to receive jobs.
        """

        logger.debug("[WORKER] hub.start()")

        if not self.do_handshake():
            logger.debug("[HUB] handshake with hub failed...")
            return -1
        logger.debug("[WORKER] handshake with hub successful, now connected.")

        self.send_stream = ZMQStream(self.send_socket)
        self.heartbeat_timer.start()
        self.job_stream.on_recv(self.poll_hub)

    def __init__(self, loop, worker, status=WorkerStatus.AVAILABLE, **kwargs):
        """
        Setup instance variables from config dictionary
        """

        logger.debug("[WORKER] initiating HubConnection instance")
        # liang: I prefer writing down all available options, because in this
        # dynamic style, reviewer has to dig into the code to figure out all
        # configurable options.
        # TODO: revisit the design here
        for option in self.defaults:
            setattr(self, option, self.defaults[option])
        for option in kwargs:
            setattr(self, option, kwargs[option])

        self.worker_status = status

        self.loop = loop
        self.worker = worker
        self.send_socket = None
        self.send_stream = None
        self.job_socket = None
        self.job_stream = None
        self.heart_socket = None
        self.heart_stream = None
