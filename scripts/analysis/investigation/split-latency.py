#!/usr/bin/python3

import os
import sys

def split_latencies(file):
    latencies_client = {}
    latencies_type = {}
    if not os.path.exists("latencies"):
        os.mkdir("latencies")
    for l in open(file):
        parts = l.split()

        # Store in client latencies
        c = parts[0]
        if latencies_client.get(c) == None:
            latencies_client[c] = open("latencies/client_" + c + ".latency", "w")
        latencies_client[c].write(l)

        # Store in type latencies
        t = parts[3]
        if latencies_type.get(t) == None:
            latencies_type[t] = open("latencies/type_" + t + ".latency", "w")
        latencies_type[t].write(l)

    for k,v in latencies_client.items():
        v.close()
    for k,v in latencies_type.items():
        v.close()

def run():
    if len(sys.argv) < 2 or not os.path.isfile(sys.argv[1]):
        print("Usage: split-latency.py <file>")
        sys.exit(1)

    split_latencies(sys.argv[1])

if __name__ == '__main__':
    run()