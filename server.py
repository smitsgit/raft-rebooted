import sys
from threading import Thread, current_thread
import time
import random
from enum import Enum
import concurrent.futures

from peer_manager import PeerManager
from config import cluster_info_by_name
from dataclasses import dataclass
from rpc_manager import RPCManager, RPCProxy


class ConsensusState(Enum):
    CANDIDATE = 0
    FOLLOWER = 1
    LEADER = 2


state_to_name = {
    0: "CANDIDATE",
    1: "FOLLOWER",
    2: "LEADER"
}

@dataclass
class RequestVote:
    term: int
    candidate_id: str
    last_log_index: int
    last_log_term: int

@dataclass
class RequestVoteResponse:
    term: int
    vote_granted: bool


def setup_rpc(server_name):
    print(f"Starting rpc for {cluster_info_by_name[server_name]}")
    rpc_mgr = RPCManager(address=cluster_info_by_name[server_name], authkey=b"peekaboo")
    return rpc_mgr


class Server:
    def __init__(self, name, peer_mgr, rpc_mgr):
        self.election_counter = 0
        self.current_term = 0
        self.state = ConsensusState.FOLLOWER
        self.name = name
        self.election_reset_event_time = time.time()
        self.election_timeout = Server.get_timeout()
        self.peer_mgr = peer_mgr
        self.rpc_mgr = rpc_mgr
        self.voted_for = None

    def start_rpc(self):
        print(f"{self.name} : listening on {self.rpc_mgr.address}")
        for item in [self.request_vote_handler, self.append_entry_handler]:
            self.rpc_mgr.register_function(item)
        self.rpc_mgr.register_listener(self)
        t = Thread(name=f"{self.name}-rpcthread", target=self.rpc_mgr.serve_forever)
        t.daemon = True
        t.start()

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
                self.start_election()
                print(f"{self.name} : Starting the election now")
                return

    @staticmethod
    def get_check_interval():
        return 1

    @staticmethod
    def get_timeout():
        return 1 + random.randint(0, 5)

    def start_election(self):
        self.become_candidate()
        print(f"{self.name} : Becomes candidate with term = {self.current_term}")
        self.request_vote_from_peers()

    def become_candidate(self):
        self.state = ConsensusState.CANDIDATE
        self.increment_the_term()
        self.reset_election_timer()
        self.voted_for = self.name

    def increment_the_term(self):
        self.current_term += 1

    def request_vote_from_peers(self):
        if self.state != ConsensusState.CANDIDATE:
            return
        votes_received = 1
        peers = {server: address for server, address in cluster_info_by_name.items() if self.name != server}

        # We can use a with statement to ensure threads are cleaned up promptly
        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
            future_to_server = {executor.submit(self.do_rpc_request, server, address): server for
                                server, address in peers.items()}
            for future in concurrent.futures.as_completed(future_to_server):
                server = future_to_server[future]
                try:
                    data = future.result()
                except Exception as exc:
                    print(f"{self.name} : [ {server} ] generated an exception {exc}")
                else:
                    print(f"{self.name} : Result from {server} of the Request Vote response {data}")
                    if self.state != ConsensusState.CANDIDATE:
                        print(
                            f"{self.name} : while waiting for the reply state changed {state_to_name[self.state]}")
                        return

                    if data.vote_granted:
                        votes_received = votes_received + 1

        print(f"{self.name} : For a current term {self.current_term} the votes received {votes_received}")
        nodes_that_can_fail = (len(cluster_info_by_name) - 1) / 2
        if votes_received > nodes_that_can_fail:
            print(f"{self.name} : Got majority of votes .. Lets roll and become leader")
            self.become_leader()

    def reset_election_timer(self):
        self.election_reset_event_time = time.time()

    def do_rpc_request(self, remote_server, address):
        result = None
        print(f"{self.name}: Sending Request Vote RPC request")
        if self.peer_mgr.peers[remote_server] is None:
            print(f"{self.name} : trying to connect to {remote_server} at {address}")
            remote = RPCProxy(cluster_info_by_name[remote_server], authkey=b"peekaboo", timeout=2)
            print(f"{self.name} : successfully connected to {remote_server}")
        else:
            remote = self.peer_mgr.peers[remote_server]
        data = RequestVote(term=self.current_term, candidate_id=self.name, last_log_index=0, last_log_term=0)
        print(f"{self.name}: Remote is {remote}")
        try:
            result = remote.request_vote_handler(data)
        except Exception as e:
            print(e)

        print(f"{self.name}: Request vote result is {result}")
        return result

    def become_leader(self):
        self.state = ConsensusState.LEADER
        print(f"{self.name} : Becomes leader with term {self.current_term}")
        # self.start_heart_beats()

    def become_follower(self, leader_term):
        self.state = ConsensusState.FOLLOWER
        self.current_term = leader_term
        self.reset_election_timer()
        self.voted_for = None
        self.run_election_timer()

    def request_vote_handler(self, request_vote):
        print(f"{self.name}: Handle the request vote {request_vote}")
        return self.handle_request_vote_response(request_vote)

    def append_entry_handler(self):
        pass

    def handle_request_vote_response(self, request_vote):
        print(f"{self.name} : handling Response for the request {request_vote}")
        if request_vote.term > self.current_term:
            print(f"{self.name} : Someone has greater term than me : Let me better be follower")
            self.become_follower(request_vote.term)

        if self.current_term == request_vote.term and \
                (self.voted_for is None or self.voted_for == request_vote.candidate_id):
            reply = RequestVoteResponse(term=self.current_term, vote_granted=True)
            self.voted_for = request_vote.candidate_id
            self.election_reset_event_time = time.time()
        else:
            reply = RequestVoteResponse(term=self.current_term, vote_granted=False)

        print(f"{self.name} : The response is {reply}")
        return reply


def main():
    server_name = f"server{sys.argv[1]}"
    print(f"Starting as : {server_name}")
    rpc_mgr = setup_rpc(server_name)
    peer_mgr = PeerManager(server_name)
    server = Server(server_name, peer_mgr, rpc_mgr)
    server.start_rpc()
    peer_mgr.connect_all_peers()
    time.sleep(15)
    print(f"Peers for {server_name}: {peer_mgr.peers}")
    server.run_election_timer()
    time.sleep(60)


if __name__ == '__main__':
    main()
