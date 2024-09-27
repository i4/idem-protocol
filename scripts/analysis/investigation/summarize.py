#!/usr/bin/python3

import os
import sys

def run():
    if len(sys.argv) < 2:
        print("Usage: summarize.py <filename>")
        sys.exit(1)
    filename = sys.argv[1]

    throughput = 0
    latency = 0
    latency_stddev = 0
    count = 0
    for dirname in os.listdir("."):
        if not os.path.isdir(dirname):
            continue
        if not os.path.isfile(dirname + "/" + filename):
            # print("Skipping " + dirname)
            continue
        print(dirname + "/" + filename)
        for line in open(dirname + "/" + filename):
            print(line)
            # ignore first line
            if (line.startswith("client_count")):
                continue
            parts = line.split()
            throughput += float(parts[1])
            latency += float(parts[4])
            latency_stddev += float(parts[6])
            count += 1
        
    # Print results
    print("\nSummary of " + str(count) + " files:")
    print("Throughput: " + str(throughput/count))
    print("Latency: " + str(latency/count))
    print("Latency Stdev: " + str(latency_stddev/count))


if __name__ == '__main__':
    run()