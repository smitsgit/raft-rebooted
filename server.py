import sys
from threading import Thread, current_thread
import time
import random
from enum import Enum

import zerorpc

from peer_manager import PeerManager
from config import cluster_info_by_name


class ConsensusState(Enum):
    CANDIDATE = 0
    FOLLOWER = 1
    LEADER = 2


state_to_name = {
    0: "CANDIDATE",
    1: "FOLLOWER",
    2: "LEADER"
}


class RPCManager:
    def hello(self):
        pass


def run_server(server_name):
    rpcServer = zerorpc.Server(RPCManager())
    print(f"Starting rpc for {cluster_info_by_name[server_name]}")
    rpcServer.bind(cluster_info_by_name[server_name])
    rpcServer.run()


def setup_rpc(server_name):
    thread = Thread(name=f"{server_name}-RPC", target=run_server, args=(server_name,), daemon=True)
    thread.start()


class Server:
    def __init__(self, name):
        self.election_counter = 0
        self.current_term = 0
        self.state = ConsensusState.FOLLOWER
        self.name = name
        self.election_reset_event_time = time.time()
        self.election_timeout = Server.get_timeout()

    def run_election_timer(self):
        t = Thread(name=f"Election Timer-{self.election_counter}", target=self.handle_election,
                   args=(self.current_term,))
        t.daemon = True
        t.start()
        self.election_counter = self.election_counter + 1

    def handle_election(self, term_started):
        print(f"{self.name} : {current_thread().name} started with Term = {self.current_term} "
              f"and timeout = {self.election_timeout}s")

        while True:
            time.sleep(self.get_check_interval())
            if self.state != ConsensusState.CANDIDATE and \
                    self.state != ConsensusState.FOLLOWER:
                print(f"{self.name} : In LEADER state, So supposed to send heartbeats")
                return

            if term_started != self.current_term:
                print(
                    f"{self.name} : {current_thread().name} : In election timer, term changed from {term_started} to {self.current_term}")
                print(f"{self.name} : This is plain wrong, bailing out...")
                return

            elapsed = time.time()

            if elapsed - self.election_reset_event_time > self.election_timeout:
                # self.start_election()
                print(f"{self.name} : Starting the election now")
                return

    @staticmethod
    def get_check_interval():
        return 1

    @staticmethod
    def get_timeout():
        return 10 + random.randint(0, 5)


def main():
    server_name = f"server{sys.argv[1]}"
    print(f"Starting as : {server_name}")
    setup_rpc(server_name)
    peer_mgr = PeerManager(server_name)
    peer_mgr.connect_all_peers()
    time.sleep(15)
    server = Server(server_name)
    server.run_election_timer()
    print(f"Peers for {server_name}: {peer_mgr.peers}")
    time.sleep(30)


if __name__ == '__main__':
    main()
