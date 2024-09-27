#!/usr/bin/env python3

import argparse
import os
import shlex
import subprocess
import sys
from configparser import ConfigParser
from typing import List

MAIN_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))


def execute(cmd_args) -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("-p", "--prio", help="Use priority queue",
                        dest="queue", default="normal", action="store_const", const="prio")
    parser.add_argument("--parent-folder", help="Enqueue the whole repository parent folder",
                        action="store_true")
    parser.add_argument("experiment_name", help="name of the experiment")

    args = parser.parse_args(cmd_args)
    return queue(args.queue, args.experiment_name, args.parent_folder)


def queue(queue_name: str, experiment_name: str, distribute_parent_folder: bool) -> None:
    scripts_dir = os.path.join(MAIN_DIR, "scripts")
    queue_file = os.path.join(scripts_dir, "config", "queue")
    config = load_config(queue_file)

    # no compression for now as this speeds up queueing and doesn't save much space
    queue_fn = "{}/{}/{}.tgz".format(config["queue.path"], queue_name, experiment_name)
    marker_fn = "{}.marker".format(queue_fn)
    commands_fn = "{}.commands".format(queue_fn)

    print("Uploading experiment to {}:{}".format(config["queue.host"], queue_fn))
    subprocess.run(["ssh", "{}@{}".format(config["queue.user"], config["queue.host"])]
                   + quote("/bin/bash", "-c", " ".join(quote("mkdir", "-p", config["queue.upload"]))),
                   stdout=subprocess.DEVNULL, cwd=MAIN_DIR, check=True)

    src = "./" if not distribute_parent_folder else "../"
    basepath = ""
    if distribute_parent_folder:
        basepath = "/" + os.path.basename(MAIN_DIR)

    subprocess.run(["rsync", "-ahe", "ssh", "--delete",
                    "--exclude=bin", "--exclude=out", "--exclude=tmp",
                    "--exclude=results", "--exclude=remote-runner",
                    "--exclude=.git", "--exclude=.idea", "--exclude=.mypy_cache", "--exclude=.terraform",
                    src, "{}@{}:{}/".format(config["queue.user"], config["queue.host"], config["queue.upload"])],
                   cwd=MAIN_DIR, check=True)

    print("Queueing experiment")
    tar_cmd = quote("tar", "-cf", queue_fn, "-C", config["queue.upload"], ".")
    cp_cmd = quote("cp", "{}/scripts/generated-config/commands".format(config["queue.upload"] + basepath), commands_fn)
    touch_cmd = quote("touch", marker_fn)
    cmd = tar_cmd + ["&&"] + cp_cmd + ["&&"] + touch_cmd
    ssh_cmd = ["ssh", "-t", "{}@{}".format(config["queue.user"], config["queue.host"])] \
              + quote("/bin/bash", "-c", " ".join(cmd))
    try:
        subprocess.run(ssh_cmd, cwd=MAIN_DIR, check=True)
    except subprocess.CalledProcessError as e:
        print(e)
        print("\nFailed to queue experiment at {}:{}"
              "\nDid you forget to start `queue_runner.py` in the matching directory?"
              .format(config["queue.host"], queue_fn))


def load_config(fn: str):
    with open(fn, "r") as f:
        data = f.read()

    cp = ConfigParser()
    cp.read_string("[DEFAULT]\n" + data)
    return cp["DEFAULT"]


def quote(*cmd) -> List[str]:
    return [shlex.quote(c) for c in cmd]


if __name__ == '__main__':
    execute(sys.argv[1:])
