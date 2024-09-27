#!/usr/bin/python3

import os
import sys

def split_rejects(file):
    results_requests = open(file + ".requests", "w")
    results_rejects = open("rejects." + file, "w")

    start = False
    for l in open(file):
        parts = l.split()

        if (not start) and "BENCH: Startup delay" in l:
            start = True
            results_requests.write(l)
            results_rejects.write(l)
            continue

        if not start:
            continue

        if "[EVENT]" in l:
            results_requests.write(l)
            results_rejects.write(l)
            if "BENCH: END:" in l:
                break
            else:
                continue

        # Main data body
        if "r" in l:
            # Reject
            results_rejects.write(l)
        else:
            results_requests.write(l)

    results_requests.close()
    results_rejects.close()

    # Rename files to match other scripts
    os.rename(file, "original."+ file)
    os.rename(file + ".requests", file)

def run():
    for dirname in os.listdir("."):
        if not os.path.isdir(dirname):
            continue
        if "rejects" in dirname or os.path.isdir(dirname.replace("refit","refit-rejects")) or not os.path.isfile(dirname + "/client-0.log"):
            print("Skipping " + dirname)
            continue
        if os.path.isfile(dirname + "/original.client-0.log"):
            os.rename(dirname + "/original.client-0.log", dirname + "/client-0.log")
        os.chdir(dirname)
        split_rejects("client-0.log")
        os.chdir("..")
        # Copy to own folder
        rejectdir = dirname.replace("refit","refit-rejects")
        os.mkdir(rejectdir)
        os.rename(dirname + "/rejects.client-0.log", rejectdir + "/client-0.log")

if __name__ == '__main__':
    run()
