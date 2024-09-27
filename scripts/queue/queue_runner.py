#!/usr/bin/env python3

# queue files like
# tar -czf "$(date +%s).tgz" run
# add <FN>.marker file to ensure atomicity of queueing

import os
import re
import shutil
import subprocess
import time
from typing import Callable, List, Optional

QUEUE_DIRECTORY = 'queue/normal'
PRIO_DIRECTORY = 'queue/prio'
FILE_PATTERN = r"^.*\.tgz$"
EXECUTION_DIRECTORY = 'runner'
SLEEP_MAX = 15


def fix_screen_env() -> None:
    if "TMUX" in os.environ:
        del os.environ["TMUX"]
        print("WARNING: Found TMUX variable from tmux\nRemoving TMUX environment variable to allow nesting")


def main_loop(queue_directories: List[str], pattern: str, execution_dir: str) -> None:
    for queue_dir in queue_directories:
        os.makedirs(queue_dir, exist_ok=True)
        print("Looking for queue entries in {}".format(os.path.abspath(queue_dir)))

    def query() -> Optional[str]:
        return find_oldest_file_multi(queue_directories, pattern)

    while True:
        full_fn = wait_for_file(query)
        execute(full_fn, execution_dir, query)


def wait_for_file(query: Callable[[], Optional[str]]) -> str:
    sleep_timeout = 1

    while True:
        full_fn = query()
        if full_fn is not None:
            return full_fn
        subprocess.call(["bash", "-c", "read -t {}".format(sleep_timeout)])
        sleep_timeout = min(SLEEP_MAX, sleep_timeout + 1)


def find_oldest_file_multi(queue_directories: List[str], pattern: str) -> Optional[str]:
    for queue_dir in queue_directories:
        file = find_oldest_file(queue_dir, pattern)
        if file is not None:
            return file
    return None


def find_oldest_file(queue_directory: str, pattern: str) -> Optional[str]:
    pattern_re = re.compile(pattern)
    highest_priority = None
    file = None
    for fn in os.listdir(queue_directory):
        full_fn = os.path.join(queue_directory, fn)
        if not pattern_re.match(fn) \
                or not os.path.exists(full_fn + ".marker") \
                or not os.path.exists(full_fn + ".commands"):
            continue

        priority = int_prefix(fn)
        if highest_priority is None or priority < highest_priority:
            highest_priority = priority
            file = full_fn

    return file


def int_prefix(string: str) -> int:
    prefix = []
    for c in string:
        if c.isdigit():
            prefix.append(c)
        else:
            break

    merged = "".join(prefix)
    try:
        return int(merged)
    except ValueError:
        return 0


def execute(full_fn: str, execution_dir: str, query: Callable[[], Optional[str]]) -> None:
    print("Starting execution of {}".format(full_fn))
    shutil.rmtree(execution_dir, ignore_errors=True)
    os.mkdir(execution_dir)
    try:
        shutil.unpack_archive(full_fn, execution_dir)
        has_completed = run_experiments(full_fn, execution_dir, query)
    except shutil.ReadError as e:
        print("Failed execution of {}".format(full_fn))
        print(e)
        has_completed = True
    shutil.rmtree(execution_dir)

    if has_completed:
        try_unlink(full_fn)
        try_unlink(full_fn + ".marker")
        try_unlink(full_fn + ".commands")
        print("Finished execution of {}".format(full_fn))
    else:
        print("Interrupted execution of {}".format(full_fn))


def try_unlink(fn: str) -> None:
    try:
        os.unlink(fn)
    except FileNotFoundError as e:
        print(e)


def run_experiments(full_fn: str, execution_dir: str, query: Callable[[], Optional[str]]) -> bool:
    progress = load_progress(full_fn)
    with open(full_fn + ".commands") as f:
        commands = [line.strip() for line in f]

    for cmd in commands[progress:]:
        # check whether something with higher priority was queued
        alt_full_fn = query()
        if not alt_full_fn == full_fn:
            return False

        ret = subprocess.call(cmd, shell=True, cwd=execution_dir)

        progress += 1
        # don't save progress if the archive was already deleted
        if os.path.exists(full_fn):
            save_progress(full_fn, progress)

        print("Return code {}".format(ret))
        if ret != 0:
            print("Press Ctrl-C to abort experiment")
            try:
                time.sleep(1)
            except KeyboardInterrupt:
                return True

    return True


def load_progress(full_fn: str) -> int:
    progress = 0
    with open(full_fn + ".marker") as f:
        try:
            progress = int(f.read().strip())
        except ValueError:
            pass
    return progress


def save_progress(full_fn: str, progress: int) -> None:
    with open(full_fn + ".marker", "w") as f:
        f.write(str(progress))


if __name__ == '__main__':
    fix_screen_env()
    main_loop([PRIO_DIRECTORY, QUEUE_DIRECTORY], FILE_PATTERN, EXECUTION_DIRECTORY)
