import MySQLdb
import random
import msgpack, zmq
from PicoManager import PicoManager
import sys, readline, code, signal

id = 1

def handle_sigchld(sig, frame):
    pass
signal.signal(signal.SIGCHLD, handle_sigchld)

db = MySQLdb.connect(host='localhost',user='root',passwd='picoparty',db='picocenter')
db.autocommit(True)

socket = zmq.Context().socket(zmq.DEALER)
socket.identity = 'register-' + str(id).zfill(2)
socket.connect('ipc://frontend.ipc')

manager = PicoManager(db, socket)

banner = """Methods available:
            manager._reset_table()
            manager.create_fake(app, ports)
            manager.create_many_fake(app, ports, num_picos)
            manager.find_available_worker([ports, ])
            manager.migrate_picoprocesses(worker_ip)"""

vars = globals().copy()
vars.update(locals())
shell = code.InteractiveConsole(vars)
shell.interact(banner=banner)
