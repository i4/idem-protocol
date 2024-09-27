#!/usr/bin/python3

import os
import sys

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: log_to_tex.py <file> [start] [end] [origin]")
        sys.exit()
    
    start = 60
    end = 180
    origin = start
    if len(sys.argv) == 5:
        start = int(sys.argv[2])-1
        end = int(sys.argv[3])+1
        origin = int(sys.argv[4])

    file = sys.argv[1]
    print("Throughput")
    for line in open(file):
        # Ignore first line
        if (line.startswith("timestamp")):
            continue
        parts = line.split()
        second = float(parts[0])
        if second < start or second > end:
            continue
        throughput = int(parts[1])
        print("(" + str(second-origin) + ", " + str(throughput) + ")")

    print("Latency")
    for line in open(file):
        # Ignore first line
        if (line.startswith("timestamp")):
            continue
        parts = line.split()
        second = float(parts[0])
        if second < start or second > end:
            continue
        latency = int(parts[2])
        print("(" + str(second-origin) + ", " + str(latency) + ")")
