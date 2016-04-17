import MySQLdb
import random
import msgpack
import string
import config
from time import sleep
logger = config.logger

################################################################################

class Picoprocess(object):
    def __init__(self, fields):
        self.pico_id, self.exec_count, self.hot, self.worker_id, self.public_ip, self.internal_ip, self.ports, self.hostname, self.customer_id = fields

class PicoManager(object):

    def _reset_table(self):
        cursor = self.db.cursor()
        for query in ["TRUNCATE picos;",
                    "TRUNCATE meta;",
                    "TRUNCATE ips;",
                    "TRUNCATE workers;",
                    "ALTER TABLE picos AUTO_INCREMENT=3",
            "INSERT INTO meta SET last_internal_ip='192.168.122.2'"]:
            print query
            cursor.execute(query)
        self.pico_counter = 3
        self.port_counter = 10003

    def _get_next_port(self):
        ret = self.port_counter
        self.port_counter += 1
        return ret

    def _get_next_internal_ip(self):
        query = "SELECT last_internal_ip FROM meta "
        cursor = self.db.cursor()
        cursor.execute(query)
        last = cursor.fetchone()[0]

        last = last.split('.')
        octets = [int(octet) for octet in last]

        octets[3] += 1
        for i in reversed(range(1,4)):
            if octets[i] > 229:
                octets[i-1] += 1
                octets[i] = 5

        new_ip = '.'.join([str(o) for o in octets])

        query = "UPDATE meta SET last_internal_ip='{0}'".format(new_ip)
        cursor.execute(query)

        return new_ip

    def _new_picoprocess(self, hostname, app, ports, customer_id=10):
        """
        Called when user registers a new picoprocess with our service.
        Simply updates the database with information about this picoprocess, and
            initiates its status to cold
        """

        cursor = self.db.cursor()

        internal_ip = self._get_next_internal_ip()
        ports = ';'.join(ports)

        # TODO INCORPORATE app
        query = "INSERT INTO picos SET exec_count={5},hot={0},internal_ip='{1}',ports='{2}',hostname='{3}',customer_id={4}".format(False,internal_ip,ports,hostname,customer_id,0)
        cursor.execute(query)

        query = "SELECT LAST_INSERT_ID();"
        cursor.execute(query)
        pico_id = cursor.fetchall()[0][0]

        return Picoprocess((pico_id, 0, False, None, None, internal_ip, ports, hostname, customer_id))

    def find_available_worker(self, ports, worker_id):
        ports = ports.split(";")

        if worker_id != 0:
            cursor = self.db.cursor()
            cursor.execute("SELECT ip, heart_ip, worker_id FROM workers WHERE worker_id={0}".format(worker_id))
            return cursor.fetchone()

        logger.debug("looking for ports: {0}".format(ports))

        port_conditions = "pp.port=%s" % ports[0]
        for port in ports[1:]:
            port_conditions += (" OR pp.port=%s" % port)

        query = "SELECT ip, heart_ip, w.worker_id FROM ips i, workers w WHERE i.worker_id=w.worker_id AND status=1 AND (ip NOT IN (SELECT public_ip FROM picos p, picoports pp WHERE p.pico_id=pp.pico_id AND ({0})))".format(port_conditions)
        cursor = self.db.cursor()
        cursor.execute(query)

        return cursor.fetchone()

    def create_picoprocess(self,  ports, worker_id=0):
        hostname = str(self.pico_counter).zfill(4) + '.pico'
        new_pico = self._new_picoprocess(hostname, ports, 10)
        self._run_picoprocess(new_pico, worker_id)
        self.pico_counter += 1
        new_pico.hostname = hostname
        return new_pico

    # TODO for now app name isn't used, need to add this to table
    def create_fake(self, app, ports, worker_id = 0):
        hostname = str(self.pico_counter).zfill(5) + '.pico'
        new_pico = self._new_picoprocess(hostname, app, ports)
        new_pico.hostname = hostname
        self.pico_counter += 1
        return new_pico

    def create_many(self, num_picos, worker_id=0, wait=2):
        ports = ["8080"]
        picos = {}

        for i in range(num_picos):
            pico = self.create_picoprocess(ports, worker_id)
            picos[pico.hostname] = pico
            sleep(wait)

        return picos

    def create_many_fake(self, app, ports, num_picos):
        picos = {}
        worker_id = 1

        for i in range(num_picos):
            pico = self.create_fake(app, ports, worker_id)
            picos[pico.hostname] = pico

        return picos


    def _run_fake_picoprocess(self, pico, resume=True, worker_id=0):
        """
        Called by DNS Resolver when it reiceves a request for a cold process.
        Chooses a suitable machine, informs worker, updates database, then responds
            to DNS query
        """

        logger.debug("PicoManager: run_picoprocess (pico={0})".format(pico.__dict__))

        host_ip = '192.168.2.10'
        heart_ip = '192.168.2.10'
        worker_id = 1

        pico.public_ip = host_ip
        pico.worker_id = worker_id

        public_port = 10000 + pico.pico_id
        pico.public_port = public_port
        logger.info("chose public port {0}".format(public_port))

        query = ("UPDATE picos SET hot=FALSE, worker_id='{0}', public_ip='{1} WHERE pico_id = '{2}".format(worker_id, public_ip, pico_id))
        cursor = self.db.cursor()
        cursor.execute(query)
        return True


    def _run_picoprocess(self, pico):
        """
        Called by DNS Resolver when it reiceves a request for a cold process.
        Chooses a suitable machine, informs worker, updates database, then responds
            to DNS query
        """

        logger.debug("PicoManager: run_picoprocess (pico={0})".format(pico.__dict__))
        result = self.find_available_worker(pico.ports, 0)
        if not result:
            logger.critical("could not find any available workers, pico remains cold...")
            return False
        host_ip, heart_ip, worker_id = result
        logger.debug("found worker {0}".format(heart_ip))

        #host_ip = '54.152.129.245'
        #heart_ip = '54.152.129.245'
        #worker_id = 1

        # TODO ...
        portmap = {}
        for port in pico.ports.split(";"):
            # portmap[port] = self._get_next_port()
            portmap[port] = 10000 + pico.pico_id
        logger.info("chose public portmap {0}".format(str(portmap)))

        pico.public_ip = host_ip
        pico.worker_id = worker_id

        # ex: pico_exec(5, 192.168.122.5, {80:1234}, 0)
        args = (pico.pico_id, pico.internal_ip, portmap, pico.exec_count)
        logger.debug(args)

        args = msgpack.packb(args)
        #### IP|DATA
        msg = heart_ip + "|" + args

	logger.debug("send to worker")
        self.socket.send(msg)

        # TODO THE FOLLOWING SHOULD BE DONE IN A SEPARATE THREAD
        # SO THAT THE DNS SERVER RESPONDS IMMEDIATELY AFER HANDLING
        # REQUEST. RIGHT NOW IT WAITS UNTIL PICO IS RUNNING TO RESPOND
        ########################################################
        ret = self.socket.recv()
        logger.debug("message recvd from worker: " + str(ret))
        if ret and int(ret) != 0:
            # TODO email customer?
            logger.critical("worker failed to start picoprocess!")
            return False

        pico.hot = True

        query = ("UPDATE picos SET hot=TRUE, worker_id='{0}', public_ip='{1}' WHERE pico_id='{2}'".format(worker_id, pico.public_ip, pico.pico_id))
        cursor = self.db.cursor()
        cursor.execute(query)
        ########################################################

        return True


    def migrate_picoprocesses(self, bad_worker):
        """
        Called by heart when a machine, bad_worker, has gone down (or
            stopped responding).
        Moves all picoprocesses from bad_worker to some new workers, which
            are not necessarily all the same, depending on port requirements
            of each process
        """
        logger.debug("PicoManager: migrate_picoprocesses({0})".format(bad_worker))

        cursor = self.db.cursor()

        find_orphans = "SELECT * FROM picos WHERE worker_id=(SELECT worker_id FROM workers WHERE heart_ip='{0}')".format(bad_worker)
        cursor.execute(find_orphans)
        orphans = cursor.fetchall()

        logger.debug("Found {0} orphans, relocating...".format(len(orphans)))

        for orphan in orphans:
            self._run_picoprocess(Picoprocess(orphan), resume=True)

        remove_worker = "DELETE FROM ips,workers USING workers INNER JOIN ips WHERE ip='{0}'".format(bad_worker)
        cursor.execute(remove_worker)

    def __init__(self, db, socket):
        self.db = db
        self.socket = socket
        self.pico_counter = 3
        self.port_counter = 10003
        # test
