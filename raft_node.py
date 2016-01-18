import time
import uuid
import random
# from enum import Enum
#
# class RaftMessageType(Enum):
#     ELECT = 1

class RaftNode(object):
    def __init__(self, id):
        self.id = id
        self.term_timeout = 2.0
        self.send_election = None
        self.elected_node_id = None
        self.elected_node_time = None

    def __repr__(self):
        return '<{} id:{}>'.format(self.__class__.__name__, self.id)

    def receive_election(self, node_id):
        self.elected_node_id = node_id
        self.elected_node_time = time.time()
        print('receive_election:', self.id, 'ELECTED', node_id)

    def _send_election(self):
        if not self.send_election:
            return

        self.send_election(self.id)

    def run(self):
        print('INIT', self.id)
        
        while True:
            print('AWAKE', self.id)
            
            if self.elected_node_id == self.id:
                self._send_election()
            else:
                if self.elected_node_id and time.time() - self.elected_node_time > self.term_timeout:
                    self.elected_node_id = None

                if not self.elected_node_id:
                    self._send_election()
            
            time.sleep(random.random() * 1.5 * self.term_timeout)
            print('SLEEP', self.id)
        
        print('DONE', self.id)

def ex1():
    node_id = str(uuid.uuid4())
    node = RaftNode(node_id)
    ss = socket.UdpServer('0.0.0.0', 7238)
    
    def receive_data(data):
        node_id, op, payload = data

        if op == 'ELECTION':
            node.receive_election(node_id)

    ss.receive_data = receive_data

    raft_node_map = {
        # node_id: (host, port),
        # node_id: (host, port),
        # node_id: (host, port),
        # node_id: (host, port),
    }

    def send_election(node_id):
        for client_host, client_port in raft_node_map.items():
            sc = socket.UdpClient(client_host, client_port)
            sc.send('ELECTION-{}'.format(node_id))

    node.send_election = send_election
    node.run()

if __name__ == '__main__':
    from concurrent import futures

    nodes = []

    def send_election(node_id):
        for node in nodes:
            node.receive_election(node_id)

    with futures.ThreadPoolExecutor(max_workers=8) as pe:
        for i in range(8):
            node_id = str(uuid.uuid4())
            node = RaftNode(node_id)
            node.send_election = send_election
            pe.submit(node.run)
            nodes.append(node)