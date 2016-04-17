import sys
import threading
from netfilterqueue import NetfilterQueue
import dpkt
import socket
import config

logger = config.logger


class FlowControlQueue:
    ''' The class creates a NetfilterQueue that blocks/releases IP packets of
    given types.
    '''

    def __init__(self, worker, queue_num=config.WorkerConfig.queue_num):
        self.worker = worker
        self.queue_num = queue_num
        self.nfqueue = None
        self.thread_recv_pkt = None
        self.lck = threading.Lock()
        self.block_table = {}  # map "$dip:$dport.$proto" -> [blocked_pkts]
        self.worker_already_informed = {}
        self._debug_flag = True  # nfqueue is sensitive of performance

    @classmethod
    def _block_name(cls, dip, dport, proto):
        return "{0}:{1}.{2}".format(dip, dport, proto)

# need parameter to say whether you should inform us based on this rule
# because that callback should only be called once
    def block(self, dip, dport, proto):
        bname = self._block_name(dip, dport, proto)
        with self.lck:
            if bname not in self.block_table:
                self.block_table[bname] = []
                self.worker_already_informed[bname] = False
        self._debug("block {0}".format(bname))

    def release(self, dip, dport, proto):
        bname = self._block_name(dip, dport, proto)
        blocked_pkts = []
        with self.lck:
            if bname in self.block_table:
                blocked_pkts = self.block_table[bname]
                del self.block_table[bname]
                del self.worker_already_informed[bname]
        n = len(blocked_pkts)
        for pkt in blocked_pkts:
            pkt.accept()
        self._debug("release {0} packets of {1}".format(n, bname))

    def _run_nfqueue(self):
        self.nfqueue = NetfilterQueue()
        self.nfqueue.bind(self.queue_num, self._process_pkt)
        try:
            self._debug("bind to queue #{0}".format(self.queue_num))
            self.nfqueue.run()
        except KeyboardInterrupt:
            self._debug("No.%s nfqueue stops.", self.queue_num)
            self.nfqueue.unbind()
            self.nfqueue = None

    def async_run_nfqueue(self):
        is_alive = self.thread_recv_pkt and self.thread_recv_pkt.is_alive()
        if is_alive or self.nfqueue:
            raise Exception("NetfilterQueue has started.")
        self.thread_recv_pkt = threading.Thread(target=self._run_nfqueue)
        self.thread_recv_pkt.daemon = True  # kill thread if main exits
        self.thread_recv_pkt.start()

    def inform_worker(self, dip, dport, proto):
        self.worker.loop.add_callback(
            lambda: self.worker.port_callback(dip, dport, proto)
            )

    def _process_pkt(self, pkt):
        # pkt has these functions
        # ['accept', 'drop', 'get_payload', 'get_payload_len',
        # 'get_timestamp', 'hook', 'hw_protocol', 'id', 'payload', 'set_mark']
        data = pkt.get_payload()
        ip = dpkt.ip.IP(data)
        # sip = socket.inet_ntop(socket.AF_INET, ip.src)
        # sport = ip.data.sport
        dip = socket.inet_ntop(socket.AF_INET, ip.dst)
        dport = ip.data.dport
        proto = "UNKNOWN"
        if ip.p == dpkt.ip.IP_PROTO_TCP:
            proto = "TCP"
        elif ip.p == dpkt.ip.IP_PROTO_UDP:
            proto = "UDP"
        bname = self._block_name(dip, dport, proto)
        blocked = False
        already_informed = False
        with self.lck:
            if bname in self.block_table:
                self.block_table[bname].append(pkt)
                blocked = True
            if bname in self.worker_already_informed:
                already_informed = self.worker_already_informed[bname]
        if (blocked and (not already_informed)):
                self.inform_worker(dip, dport, proto)
                self.worker_already_informed[bname] = True
        else:
            pkt.accept()
        self._debug("_process_pkt {0}".format(bname))

    def _debug(self, msg):
        if self._debug_flag:
            logger.debug("[FlowControlQueue] "+msg)
