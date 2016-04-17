import tornado
import tornado.tcpserver

import sys, traceback
import StringIO

import config, iptables
logger = config.logger

class TcpEvaluator:
    def __init__(self, stream, address, server):
        self.stream = stream
        self.address = address
        self.server = server
        stream.set_close_callback(self.close_callback)

    def close_callback(self):
        logger.debug('Commander exit %s', self.address)

    def read_line(self, data):
        # logger.debug('read_line')
        try:
            if data.strip():
                if data.strip() is not 'exit':
                    worker = self.server.worker
                    result = eval(data)
                    self.write(str(result))
                    self.write('\n')
                else:
                    self.write('bye\n')
                    self.stream.close()
        except Exception as e:
            buf = StringIO.StringIO()
            traceback.print_exc(file=buf)
            self.write(buf.getvalue())
        self.read_next()

    def read_next(self):
        # logger.debug('read_next')
        # self.write('>>> ')
        self.stream.read_until('\n', self.read_line)

    def write(self, data):
        self.stream.write(data)

class TcpEvalServer(tornado.tcpserver.TCPServer):

    def __init__(self, worker, *args, **kwargs):
        super(TcpEvalServer, self).__init__(*args, **kwargs)
        self.worker = worker

    def handle_stream(self, stream, address):
        logger.debug('New commander %s', address)
        e = TcpEvaluator(stream, address, self)
        e.read_next()

