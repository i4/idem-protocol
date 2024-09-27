#!/usr/local/bin/python3

from heapq import heappop, heappush
from itertools import permutations
from typing import Dict

GENERATE_LATEX = False

NODE_IPS = {
    -1: "54.229.227.200",  # eu-west-1, Ireland
    -2: "52.67.99.173",  # sa-east-1, Sao Paolo
    -3: "52.62.106.124",  # ap-southeast-2, Sydney
    -4: "13.127.215.219",  # ap-south-1, Mumbai
    0: "18.188.25.197",  # us-east-2, Ohio
    1: "13.57.230.75",  # us-west-1, California
    2: "52.66.166.166",  # ap-south-1, Mumbai
    3: "13.231.132.83",  # ap-northeast-1, Tokyo
}

PING_TIME_MAP = {}  # type: Dict[str, Dict[str, float]]
for ip in NODE_IPS.values():
    PING_TIME_MAP[ip] = {}
    PING_TIME_MAP[ip][ip] = 0

PING_TIME_MAP['52.67.99.173']['54.229.227.200'] = 183.993
PING_TIME_MAP['52.62.106.124']['54.229.227.200'] = 280.667
PING_TIME_MAP['13.127.215.219']['54.229.227.200'] = 120.889
PING_TIME_MAP['18.188.25.197']['54.229.227.200'] = 84.718
PING_TIME_MAP['13.57.230.75']['54.229.227.200'] = 135.278
PING_TIME_MAP['52.66.166.166']['54.229.227.200'] = 122.059
PING_TIME_MAP['13.231.132.83']['54.229.227.200'] = 226.772
PING_TIME_MAP['52.62.106.124']['52.67.99.173'] = 320.761
PING_TIME_MAP['13.127.215.219']['52.67.99.173'] = 302.122
PING_TIME_MAP['18.188.25.197']['52.67.99.173'] = 129.205
PING_TIME_MAP['13.57.230.75']['52.67.99.173'] = 179.848
PING_TIME_MAP['52.66.166.166']['52.67.99.173'] = 302.552
PING_TIME_MAP['13.231.132.83']['52.67.99.173'] = 262.607
PING_TIME_MAP['13.127.215.219']['52.62.106.124'] = 230.319
PING_TIME_MAP['18.188.25.197']['52.62.106.124'] = 193.957
PING_TIME_MAP['13.57.230.75']['52.62.106.124'] = 146.023
PING_TIME_MAP['52.66.166.166']['52.62.106.124'] = 225.374
PING_TIME_MAP['13.231.132.83']['52.62.106.124'] = 107.021
PING_TIME_MAP['18.188.25.197']['13.127.215.219'] = 190.78
PING_TIME_MAP['13.57.230.75']['13.127.215.219'] = 240.576
PING_TIME_MAP['52.66.166.166']['13.127.215.219'] = 0.513
PING_TIME_MAP['13.231.132.83']['13.127.215.219'] = 126.888
PING_TIME_MAP['13.57.230.75']['18.188.25.197'] = 49.441
PING_TIME_MAP['52.66.166.166']['18.188.25.197'] = 192.215
PING_TIME_MAP['13.231.132.83']['18.188.25.197'] = 163.886
PING_TIME_MAP['52.66.166.166']['13.57.230.75'] = 240.45
PING_TIME_MAP['13.231.132.83']['13.57.230.75'] = 114.225
PING_TIME_MAP['13.231.132.83']['52.66.166.166'] = 126.911


def make_map_reflexive(latencies):
    for sender, dest_map in latencies.items():
        for dest, delay in dest_map.items():
            if sender not in latencies[dest]:
                latencies[dest][sender] = delay


make_map_reflexive(PING_TIME_MAP)

INIT = "init"
REQUEST = "request"
PREPREPARE = "preprepare"
PREPARE = "prepare"
COMMIT = "commit"
REPLY = "reply"


class InvalidMessage(Exception):
    def __init__(self, msg):
        super().__init__()
        self.msg = msg

    def __str__(self):
        return self.msg


class Message:
    def __init__(self, msgtype, sender, dest):
        self.msgtype = msgtype
        self.sender = sender
        self.dest = dest
        self.sendtime = None

    def __str__(self):
        return "{1} -> {2} {0}".format(self.msgtype, self.sender, self.dest)

    def __lt__(self, other):
        return self.msgtype < other.msgtype


class Replica:
    REPLICA_COUNT = 4
    LEADER_ID = 0
    PREPARE_QUORUM = 3
    COMMIT_QUORUM = 3
    # set to replica id to enable
    PASSIVE_REPLICA = None

    def __init__(self, my_id, my_ip):
        self.id = my_id
        self.my_ip = my_ip
        self.step = None
        self.prepares = set()
        self.commits = set()

    def handle(self, message, time):
        if self.id == self.PASSIVE_REPLICA:
            return []
        if message.msgtype == REQUEST and self.id == self.LEADER_ID:
            return [Message(PREPREPARE, self.id, i) for i in range(self.REPLICA_COUNT)]
        elif message.msgtype == PREPREPARE and message.sender == self.LEADER_ID:
            self.step = PREPREPARE
            self.prepares.add(message.sender)
        elif message.msgtype == PREPARE and not message.sender == self.LEADER_ID:
            self.prepares.add(message.sender)
        elif message.msgtype == COMMIT:
            self.commits.add(message.sender)
        else:
            raise InvalidMessage("Replica {}: Unexpected message: {}".format(self.id, str(message)))

        msgs = []
        if self.step == PREPREPARE:
            if GENERATE_LATEX:
                print("\\node[state] (PP-{}) at ($(X-{}) + (Y-{})$) {{}};".format(self.id, int(time), 3 - self.id))
            self.step = PREPARE
            if not self.LEADER_ID == self.id:
                msgs += [Message(PREPARE, self.id, i) for i in range(self.REPLICA_COUNT)]

        if self.step == PREPARE and len(self.prepares) >= self.PREPARE_QUORUM:
            if GENERATE_LATEX:
                print("\\node[state] (P-{}) at ($(X-{}) + (Y-{})$) {{}};".format(self.id, int(time), 3 - self.id))
            self.step = COMMIT
            msgs += [Message(COMMIT, self.id, i) for i in range(self.REPLICA_COUNT)]

        if self.step == COMMIT and len(self.commits) >= self.COMMIT_QUORUM:
            if GENERATE_LATEX:
                print("\\node[state] (C-{}) at ($(X-{}) + (Y-{})$) {{}};".format(self.id, int(time), 3 - self.id))
            self.step = REPLY
            msgs += [Message(REPLY, self.id, -1)]

        return msgs


class Client:
    REPLY_QUORUM = 2

    def __init__(self, my_id, my_ip):
        self.id = my_id
        self.my_ip = my_ip
        self.replies = set()

    def handle(self, message, time):
        if message.msgtype == INIT and message.sender is None:
            return [Message(REQUEST, self.id, Replica.LEADER_ID)]
        elif message.msgtype == REPLY and message.sender >= 0:
            old_len = len(self.replies)
            self.replies.add(message.sender)
            cur_len = len(self.replies)
            if old_len != cur_len and cur_len == self.REPLY_QUORUM:
                if GENERATE_LATEX:
                    print("\\node[state] (Z) at ($(X-{}) + (Y-{})$) {{}};".format(int(time), 3 - self.id))
                return None

        return []


def insert_message(messages, timestamp, message):
    heappush(messages, (timestamp, message))


def message_loop(messages, replicas, trace=False):
    time_to_finish = None
    while len(messages) > 0:
        (time, message) = heappop(messages)

        if trace:
            print("[{:.3f}] {}".format(time, str(message)))
        if GENERATE_LATEX and message.sender is not None and message.sender != message.dest \
                and message.dest != Replica.PASSIVE_REPLICA:
            tp = "message"
            if Replica.PASSIVE_REPLICA is not None:
                tp = "calculation"
            print("\\draw[{}] ($(X-{}) + (Y-{})$) -- ($(X-{}) + (Y-{})$);"
                  .format(tp, int(message.sendtime), 3 - message.sender, int(time), 3 - message.dest))

        if message.dest not in replicas:
            print("Unknown message destination: {}".format(str(message)))
            continue

        receiver = replicas[message.dest]
        output = receiver.handle(message, time)
        if output is None:
            if trace:
                print("Success!")
            time_to_finish = time
            continue

        for sent in output:
            src_ip = replicas[sent.sender].my_ip
            dest_ip = replicas[sent.dest].my_ip
            msg_delay = PING_TIME_MAP[src_ip][dest_ip] / 2
            sent.sendtime = time
            # if trace:
            #     print("    * {} delay: {}".format(sent, msg_delay))
            insert_message(messages, time + msg_delay, sent)

    return time_to_finish


def iter_shift_left(values):
    for i in range(len(values)):
        yield values[i:] + values[:i]


def leader_passive_permutation(values, passive_count):
    for perm in permutations(values, 1 + passive_count):
        remainder = [v for v in values if v not in perm]
        yield [perm[0]] + remainder + list(perm[1:])


def run():
    replica_ips = [NODE_IPS[i] for i in range(4)]
    for ip_perm in iter_shift_left(replica_ips):
        # for ip_perm in leader_passive_permutation(replica_ips, 1):
        # for ip_perm in [replica_ips]:
        print(ip_perm)

        replicas = dict([(i, Replica(i, ip_perm[i])) for i in range(Replica.REPLICA_COUNT)])
        replicas[-1] = Client(-1, NODE_IPS[-1])

        messages = []
        insert_message(messages, 0, Message(INIT, None, -1))

        timing = message_loop(messages, replicas)

        if GENERATE_LATEX:
            print("")
            last = 0
            if Replica.LEADER_ID == 0:
                last = 1
            for i in range(Replica.REPLICA_COUNT - 1):
                if i + 1 != Replica.LEADER_ID:
                    print("\\draw[statelink] (PP-{}.center) -- (PP-{}.center);".format(last, i + 1))
                    last = i + 1
            print("")
            for i in range(Replica.REPLICA_COUNT - 1):
                print("\\draw[statelink] (P-{}.center) -- (P-{}.center);".format(i, i + 1))
            print("")
            for i in range(Replica.REPLICA_COUNT - 1):
                print("\\draw[statelink] (C-{}.center) -- (C-{}.center);".format(i, i + 1))
            print("")

        print("Agreement in {} ms".format(timing))


if __name__ == '__main__':
    run()
