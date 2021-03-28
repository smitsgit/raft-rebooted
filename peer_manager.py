import time
from threading import Thread
from config import cluster_info_by_name
import zerorpc
from rpc_manager import RPCProxy


class PeerManager:
    def __init__(self, server_name):
        self.peers = {}
        self.name = server_name

    def connect_rpc_clients(self):
        while True:
            for server, address in cluster_info_by_name.items():
                if server != self.name:
                    if self.peers.get(server, None) is None:
                        print(f"{self.name} : trying to connect to {server} at {address}")
                        try:
                            remote = RPCProxy(cluster_info_by_name[server], authkey=b"peekaboo", timeout=2)
                        except Exception as e:
                            print(f"{self.name} : [ {server} ] generated an exception: {e}")
                        else:
                            self.peers[server] = remote

            time.sleep(2)

    def connect_all_peers(self):
        Thread(name="PeerThread", target=self.connect_rpc_clients, daemon=True).start()
