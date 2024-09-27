#!/usr/bin/env python3
import argparse
import errno
import io
import os
import shlex
import shutil
import subprocess
import sys
import time
import traceback
from configparser import ConfigParser
from datetime import datetime
from typing import List, Sequence, Set, Optional, Tuple

MAIN_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
DEFAULT_RESULTS_DIR = os.path.realpath(os.path.join(MAIN_DIR, "results"))
# Set necessary correct locales
os.environ["LC_ALL"] = "C"


def build_test_run_name(testfile, scenario, args):
    name = "-".join([testfile, scenario, *args])
    timestamp = datetime.now().strftime("%Y_%m_%d-%H_%M_%S")
    return timestamp + "-" + name


class FlattenException(Exception):
    def __init__(self, collisions):
        super().__init__(collisions)
        self.collisions = collisions

    def __str__(self):
        return "Colliding file names: {}".format(str(self.collisions))


def flatten_directory(folder_name):
    ensure_safe_flatten(folder_name)
    first_level = True
    for root, dirs, files in os.walk(folder_name):
        if first_level:
            first_level = False
            continue

        for file in files + dirs:
            os.rename(os.path.join(root, file), os.path.join(folder_name, file))

        try_rmdir(root)

        # prevent recursion
        dirs.clear()


def ensure_safe_flatten(folder_name):
    used_names = set()
    collisions = set()
    first_level = True
    for _, dirs, files in os.walk(folder_name):
        items = set(dirs) | set(files)
        collisions |= items & used_names
        used_names |= items

        if first_level:
            first_level = False
        else:
            dirs.clear()

    if collisions:
        raise FlattenException(collisions)


def try_rmdir(folder_name):
    try:
        os.rmdir(folder_name)
    except OSError as e:
        if e.errno == errno.ENOTEMPTY:
            pass
        else:
            raise


def redirect_output(fn):
    tee = subprocess.Popen(["tee", "-i", fn], stdin=subprocess.PIPE, bufsize=0)
    # properly redirect subprocess output
    os.dup2(tee.stdin.fileno(), sys.stdout.fileno())
    os.dup2(tee.stdin.fileno(), sys.stderr.fileno())
    # prevent python internal buffering
    sys.stdout = io.TextIOWrapper(tee.stdin, encoding='utf-8', write_through=True)
    sys.stderr = sys.stdout


class UnknownStageException(Exception):
    pass


class TestRun:
    def __init__(self, testfile, main_dir, results_dir, args):
        self.testfile = testfile
        self.main_dir = main_dir
        self.results_dir = os.path.join(self.main_dir, results_dir)
        self.tmp_dir = os.path.join(self.main_dir, "tmp")
        self.args = args
        self.current_stage = ""
        self.config_files = []
        self.config = {}
        self.screens = []

    def run_stage(self, stage) -> bool:
        self.current_stage = stage
        func = getattr(self, "stage_{}".format(stage.lower()), None)
        if func is None:
            raise UnknownStageException()

        try:
            # pylint: disable=E1102
            return func()
        except KeyboardInterrupt:
            self.log("Interrupted")
            return False
        finally:
            self.clear_screens()
            self.current_stage = ""

    def cmd(self, *cmds, cwd=None, stdout=None) -> bool:
        if cwd is None:
            cwd = self.main_dir
        proc = subprocess.run(cmds, cwd=cwd, stdout=stdout, stderr=subprocess.STDOUT)
        return proc.returncode == 0

    def cmd_live_log(self, log_file, *cmd, cwd=None) -> bool:
        if cwd is None:
            cwd = self.main_dir
        with open(log_file, "wb") as log, \
                io.TextIOWrapper(log, encoding='utf8') as logger, \
                subprocess.Popen(cmd, stdout=subprocess.PIPE,
                                 stderr=subprocess.STDOUT, cwd=cwd) as proc, \
                io.TextIOWrapper(proc.stdout, encoding='utf8') as reader:
            while True:
                output = reader.read(1)
                if not output:
                    break
                print(output, end='')
                logger.write(output)
        return proc.returncode == 0

    def log(self, msg, stage=None):
        if stage is None:
            stage = self.current_stage
        print("[{}] {}".format(stage, msg))

    def run(self):
        if not self.run_stage("PARAM"):
            return False

        # ensure that we start with a clean tmp directory
        shutil.rmtree(self.tmp_dir, ignore_errors=True)
        os.makedirs(self.tmp_dir, exist_ok=True)

        self.config = self.load_configs(*self.config_files)

        # BUILD [DISTRIBUTE] BENCH [STORE] ANALYZE
        # BUILD: setup binaries, prepare for execution
        build_successful = self.run_stage("BUILD")
        bench_successful = False
        if build_successful:
            # reload config
            self.config = self.load_configs(*self.config_files)

            # DISTRIBUTE (generic): send data to servers, also removes leftover files
            # must reload servers after build step has completed!
            distrib_successful = self.run_stage("DISTR")
            if distrib_successful:
                # BENCH: Run benchmark, processes must be stopped afterwards
                bench_successful = self.run_stage("BENCH")
            else:
                self.log("Skipped due to fault during distribution stage", stage="BENCH")
        # STORE (generic): collect results
        os.makedirs(self.results_dir, exist_ok=True)
        # prevent `exp result` from copying results while store is still in progress
        rsync_filter_fn = os.path.join(self.results_dir, ".rsync-filter")
        with open(rsync_filter_fn, "w") as f:
            f.write("- *\n")
        try:
            self.run_stage("STORE")
        finally:
            os.unlink(rsync_filter_fn)
        shutil.rmtree(self.tmp_dir)

        if bench_successful:
            # ANALYZE: whatever
            self.run_stage("CHECK")

        return bench_successful

    def stage_param(self) -> bool:
        return True

    @classmethod
    def setup_args(cls, parser: argparse.ArgumentParser) -> None:
        pass

    @classmethod
    def load_config(cls, fn):
        with open(fn, "r") as f:
            data = f.read()

        cp = ConfigParser()
        cp.read_string("[DEFAULT]\n" + data)
        return cp["DEFAULT"]

    @classmethod
    def load_configs(cls, *fns):
        config = {}
        for fn in fns:
            config.update(cls.load_config(fn))
        return config

    @classmethod
    def wait_procs(cls, procs, kill_on_error=True):
        error = False

        for proc in procs:
            ret = proc.wait()
            if ret != 0:
                error = True
                if kill_on_error:
                    break

        if kill_on_error and error:
            for proc in procs:
                proc.kill()

        return not error

    @classmethod
    def silent_cmd(cls, *cmd):
        return subprocess.run(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.STDOUT).returncode

    def open_screen(self, name, *cmd):
        screen = ["tmux", "new-window", "-d", "-n", name, "/bin/bash", "-c", " ".join([shlex.quote(c) for c in cmd])]
        subprocess.run(screen, cwd=self.main_dir)
        self.screens.append(name)

    def wait_screen(self, timeout, screens=None):
        if screens is None:
            screens = self.screens
        dot_printed = False

        for _ in range(timeout):
            sys.stdout.flush()
            # tmux list-windows -F #W
            ret = subprocess.run(["tmux", "list-windows", "-F", "#W"], stdout=subprocess.PIPE)
            screens_available = ret.stdout.decode("utf-8").splitlines()
            screens_alive = set(screens) & set(screens_available)
            if not screens_alive:
                break

            time.sleep(1)
            print(".", end='')
            dot_printed = True

        if dot_printed:
            print("")

    def close_screen(self, screens):
        for pane in screens:
            self.silent_cmd("tmux", "send-keys", "-t", pane, "C-c")

        self.wait_screen(30, screens)

    def clear_screens(self):
        if not self.screens:
            return
        self.log("Closing screens")
        self.close_screen(self.screens)
        self.screens.clear()

    def run_blocking(self, *cmd):
        return subprocess.run(cmd, cwd=self.main_dir)


class GenericTestRun(TestRun):
    DISTRIBUTE_TO_TYPES = ["client.network.addresses", "replica.network.addresses"]  # type: Sequence[str]
    REMOTE_SSH_PARAMS = ["-o", "StrictHostKeyChecking=no", "-o", "ConnectTimeout=5"]
    RSYNC_EXCLUDES = ["--exclude=results", "--exclude=.git", "--exclude=.idea", "--exclude=.mypy_cache",
                      "--exclude=.terraform"]
    SKIP_MAKE = False

    def __init__(self, *args):
        super().__init__(*args)
        self.scripts_dir = os.path.join(self.main_dir, "scripts")
        self.server_config = os.path.join(self.scripts_dir, "config", "servers")
        self.refit_default_config = os.path.join(self.scripts_dir, "config", "refit-defaults")
        self.refit_overrides_config = os.path.join(self.scripts_dir, "config", "refit-overrides")
        self.config_files += [self.server_config, self.refit_default_config, self.refit_overrides_config]
        # additional files to store in results directory, paths should start with self.main_dir or self.scripts_dir
        self.extra_files = []
        # set to True to also distribute the parent folder. All test run commands still
        # use this repository as their working directory
        self.distribute_parent_folder = False
        self.has_distributed = False

    def get_servers(self, server_types: Sequence[str]) -> List[str]:
        servers = []  # type: List[str]
        for server_type in server_types:
            if server_type in self.config:
                # manually map servers
                raw_servers = [v.strip() for v in self.config[server_type].split(",") if v.strip()]
                for server in raw_servers:
                    servers.append(server)

        return servers

    def resolve_server(self, name: str) -> Tuple[str, bool]:
        if name + "real" in self.config:
            addr = self.config[name + "real"]
            netns = True
        else:
            addr = self.config[name]
            netns = False
        return addr, netns

    def get_unique_servers(self) -> Set[str]:
        reverse_map = {}
        servers = self.get_servers(self.DISTRIBUTE_TO_TYPES)
        for s in servers:
            sn = self.resolve_server(s)[0]
            if sn not in reverse_map:
                reverse_map[sn] = s

        return set(reverse_map.values())

    def get_unique_real_servers(self) -> Set[str]:
        return set([self.resolve_server(s)[0] for s in self.get_servers(self.DISTRIBUTE_TO_TYPES)])

    def stage_build(self) -> bool:
        self.log("Building code")
        build_dir = os.path.join(self.tmp_dir, "build")
        os.makedirs(build_dir, exist_ok=True)

        for config_file in self.config_files + self.extra_files:
            shutil.copy(config_file, build_dir)

        if self.SKIP_MAKE:
            return True

        build_log = os.path.join(build_dir, "build.log")
        if not self.cmd_live_log(build_log, "make"):
            return False

        os.unlink(build_log)
        return True

    def get_clean_paths(self, ignore_parent: bool = False):
        local_path = remote_path = self.config["remote.path"]  # type: str

        if self.distribute_parent_folder and not ignore_parent:
            base = os.path.basename(MAIN_DIR)
            local_path = os.path.join(local_path, base)
            remote_path = os.path.join(remote_path, base)

        if local_path.startswith("~/"):
            # expand to ensure local commands are executed consistently
            local_path = os.path.expanduser(local_path)
            # rsync / ssh start in the users home
            remote_path = remote_path[2:]

        if local_path.find("{}") >= 0:
            local_path = local_path.replace("{}", self.config["remote.user"])
            remote_path = remote_path.replace("{}", self.config["remote.user"])

        return local_path, remote_path

    def stage_distr(self) -> bool:
        local_path, remote_path = self.get_clean_paths(ignore_parent=True)

        # create folders in parallel
        mkdirs = []
        for server in self.get_unique_real_servers():
            if server == "localhost":
                os.makedirs(local_path, exist_ok=True)
            else:
                mkdirs.append(self.start_ssh(self.config["remote.user"], server,
                                             "mkdir", "-p", shlex.quote(remote_path)))
        res = self.wait_procs(mkdirs)
        if not res:
            return res

        # start rsyncs in parallel
        rsyncs = []
        src = "./" if not self.distribute_parent_folder else "../"
        for server in self.get_unique_real_servers():
            if server == "localhost":
                rsyncs.append(self.start_rsync(src, local_path,
                                               delete=True, remote=False))
            else:
                rsyncs.append(self.start_rsync(src, "{}@{}:{}".format(self.config["remote.user"],
                                                                      server, shlex.quote(remote_path)),
                                               delete=True))

        self.has_distributed = True
        return self.wait_procs(rsyncs)

    def start_rsync(self, src, dst, delete=False, remote=True):
        cmd = ["rsync", "-a", "--exclude", self.config["remote.path"]] + self.RSYNC_EXCLUDES
        if delete:
            cmd += ["--delete"]
        if remote:
            cmd += ["-e", " ".join(["ssh"] + self.REMOTE_SSH_PARAMS)]
        cmd += [src, dst]
        return subprocess.Popen(cmd, cwd=self.main_dir)

    def start_ssh(self, user, server, *cmd, stdout=None):
        # Note: the caller must take care of cmd quoting itself!
        cmd = ["ssh", *self.REMOTE_SSH_PARAMS, "{}@{}".format(user, server), *cmd]
        return subprocess.Popen(cmd, cwd=self.main_dir, stdout=stdout, stderr=subprocess.STDOUT)

    def stage_store(self) -> bool:
        # only collect server data if build was successful
        if self.has_distributed:
            local_path, remote_path = self.get_clean_paths()

            ok = False
            # retry once
            for i in range(2):
                # start rsyncs in parallel
                rsyncs = []
                for server in self.get_unique_real_servers():
                    if server == "localhost":
                        rsyncs.append(self.start_rsync("{}/tmp/".format(local_path),
                                                       self.results_dir, remote=False))
                    else:
                        rsyncs.append(self.start_rsync("{}@{}:{}/tmp/".format(self.config["remote.user"],
                                                                              server, remote_path), self.results_dir))

                if self.wait_procs(rsyncs):
                    ok = True
                    break

            if not ok:
                return False

        # copy test runner data last to avoid overwriting the log file
        if not self.wait_procs([self.start_rsync("tmp/", self.results_dir, remote=False)]):
            return False

        # Only flatten directories after the data collection completed
        try:
            flatten_directory(self.results_dir)
        except FlattenException as e:
            print(e)

        return True

    def stage_check(self) -> bool:
        return True


class JavaTestRun(GenericTestRun):
    DISTRIBUTE_TO_TYPES = ["client.network.addresses", "replica.network.addresses"]

    def __init__(self, *args):
        super().__init__(*args)
        self.java_config = os.path.join(self.scripts_dir, "config", "java")
        self.config_files += [self.java_config]

    def gen_java_cmd(self, identifier: str, *args) -> Sequence[str]:
        """Generate the command to call java with the given arguments. The result is a list of strings
        for example ['java', '-cp', 'example.jar', 'example.main']. No further shell tokenization will
        take place, that is a parameter '1 2 3' will be passed as one parameter, that contains spaces,
        to the application."""
        id_output = os.path.join("tmp", identifier)
        # java flight recorder
        id_jfr = os.path.join(id_output, identifier + ".jfr")

        cmd = [*shlex.split(self.config["java"])]
        profile = shlex.split(self.config["java.profile"])
        if profile:
            cmd += profile[:-1] + [profile[-1] + id_jfr]
        cmd += ["-cp", self.config["java.classpath"]]
        cmd += args
        return cmd

    def gen_remote_cmd(self, identifier: str, duration: int, server_name: str, *cmd, extra_timeout: int = 10) \
            -> Sequence[str]:
        id_output = os.path.join("tmp", identifier)

        timeout = int(duration) + extra_timeout
        local_path, remote_path = self.get_clean_paths()
        server, use_netns = self.resolve_server(server_name)
        path = local_path if server == "localhost" else remote_path

        prefix = [os.path.join(path, "scripts", "test", "helper", "exec_helper"),
                  path, os.path.join(path, id_output), identifier]
        if use_netns:
            prefix += ["netns", server_name]
        else:
            prefix.append("timer")
        prefix.append(str(timeout))

        cmd = prefix + list(cmd)

        if not server == "localhost":
            wrapper = ["ssh", "-t", *self.REMOTE_SSH_PARAMS,
                       "{}@{}".format(self.config["remote.user"], server)]
            quoted_cmd = " ".join([shlex.quote(c) for c in cmd])
            cmd = wrapper + [quoted_cmd]

        return cmd

    def gen_remote_java_cmd(self, identifier: str, duration: int, server: str, *args, extra_timeout: int = 10,
                            java_prefix: Sequence[str] = ()) -> Sequence[str]:
        java_cmd = self.gen_java_cmd(identifier, *args)
        # The java_prefix can be used to inject additional commands before the java call
        # It must be an already tokenized list of arguments, for example java_prefix=['taskset', '42']
        res = self.gen_remote_cmd(identifier, duration, server, *java_prefix, *java_cmd, extra_timeout=extra_timeout)
        return res


def execute(cls, cmd_args):
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest="cmd")

    run_parser = subparsers.add_parser('run', help='run benchmark')
    run_parser.add_argument("-t", "--tmuxed", help="Internal parameter, signals that benchmark already runs inside "
                                                   "a tmux session. This is also useful when debugging start-up errors "
                                                   "which are swallowed by tmux.", action="store_true")
    run_parser.add_argument("-r", "--resultsdir", help="path to the results directory", default=DEFAULT_RESULTS_DIR)
    run_parser.add_argument("testfile", help="test script which controls the benchmark execution")
    run_parser.add_argument("scenario", help="name of the current test scenario")
    cls.setup_args(run_parser)

    queue_parser = subparsers.add_parser('queue', help='queue benchmark')
    queue_parser.add_argument("-p", "--prio", help="Use priority queue", action="store_true")
    queue_parser.add_argument("-m", "--multi", help="Generate variants", action="store_true")
    queue_parser.add_argument("-n", "--dry-run", help="Prepare only benchmark execution", action="store_true")
    queue_parser.add_argument("-c", "--check", help="Run configuration check for every experiment variant. "
                                                    "Implies dry-run", action="store_true")
    queue_parser.add_argument("--parent-folder", help="Enqueue the whole repository parent folder",
                              action="store_true")
    queue_parser.add_argument("-a", "--add-analyze", help="run the specified analysis task at the end", action='append')
    queue_parser.add_argument("testfile", help="test script which controls the benchmark execution")
    queue_parser.add_argument("scenario", help="name of the current test scenario")
    queue_parser.add_argument("extra", help="further parameters", nargs='*')

    queue_parser = subparsers.add_parser('result', help='poll results of queued benchmarks')
    queue_parser.add_argument("path", help="destination of results")

    args = parser.parse_args(cmd_args[1:])
    if args.cmd == "run":
        # File format: first line = exit code; second line = error log file; further lines = error output
        exit_code_fn = os.path.join(MAIN_DIR, args.resultsdir, "EXIT_CODE")
        if args.tmuxed:
            subprocess.run(["tmux", "set-option", "status-left", ""])
            subprocess.run(["tmux", "set-option", "status-right", "{} [{}] ".format(args.scenario, args.testfile)])
            try:
                successful = run(cls, args, exit_code_fn)
            except BaseException:
                try:
                    ex = sys.exc_info()
                    with open(exit_code_fn, "a") as f:
                        # we can't know whether run(...) has already prepared the exit_code file
                        # so just add enough newlines to be safe
                        f.write("\n\n" + "".join(traceback.format_exception(*ex)))
                except IOError as e2:
                    # now we're out of options, just print it
                    print(e2)
                # throw upwards
                raise
            code = int(not successful)

            # store exit code as it can't be passed through screen/tmux
            with open(exit_code_fn, "w") as f:
                f.write(str(code))

            return code
        else:
            try:
                os.unlink(exit_code_fn)
            except FileNotFoundError:
                pass
            # create exit code file, to ensure we can write to the results dir
            os.makedirs(os.path.join(MAIN_DIR, args.resultsdir), exist_ok=True)
            with open(exit_code_fn, "w") as f:
                f.write("")

            tmux_exit = subprocess.run(["tmux", "new-session", "-n", "main", "-s", "bench",
                                        *cmd_args[0:2], "--tmuxed", *cmd_args[2:]])
            if tmux_exit.returncode != 0:
                # It is sometimes possible to interrupt the tmux startup such that the server keeps running
                # but the client exists. This will block experiment runs until the interrupted experiment
                # completes.
                print("Failed to start tmux. Maybe another experiment is already running?\n"
                      "Please try to attach using `tmux attach -t bench` and investigate.")

            with open(exit_code_fn, "r") as f:
                exit_data = f.read()

            exit_val = exit_data.split("\n", 2)
            if exit_val[0]:
                return int(exit_val[0])

            # try to print log file
            if len(exit_val) >= 2 and exit_val[1]:
                try:
                    with open(exit_val[1], "r") as f:
                        print(f.read())
                except IOError:
                    print("Failed to read error log file " + exit_val[1])

            # print additional error messages
            if len(exit_val) >= 3 and exit_val[2]:
                print(exit_val[2].strip())

            # return error
            return 1
    elif args.cmd == "queue":
        experiment_name = gen_experiment_name(args)
        if args.check:
            args.dry_run = True
        success = prepare(args, experiment_name) and (
                args.dry_run or queue(args.prio, experiment_name, args.parent_folder))
        if success:
            return 0
        else:
            return 255

    elif args.cmd == "result":
        poll_results(args.path)
        return 0

    else:
        parser.print_usage()
        return 255


def gen_experiment_name(args):
    experiment_name = str(int(time.time()))
    if args.scenario:
        experiment_name = "{}-{}".format(experiment_name, args.scenario)
    return experiment_name


def prepare(args, experiment_name):
    scripts_dir = os.path.join(MAIN_DIR, "scripts")
    queue_file = os.path.join(scripts_dir, "config", "queue")
    config = TestRun.load_config(queue_file)

    if not os.path.exists(os.path.join(scripts_dir, "test/{}.py".format(args.testfile))):
        print("Unable to locate test script {}".format(args.testfile))
        return False

    genconfig_dir = os.path.join(scripts_dir, "generated-config")
    os.makedirs(genconfig_dir, exist_ok=True)
    subprocess.run(["make", "clean"], cwd=MAIN_DIR, check=True, capture_output=True)

    shutil.copy(os.path.join(scripts_dir, "config/refit-defaults"), genconfig_dir)
    shutil.copy(os.path.join(scripts_dir, "config/refit-overrides"), genconfig_dir)

    with open(os.path.join(genconfig_dir, "git-info"), "w") as f:
        f.write("Running revision: {}\n\n".format(
            subprocess.run(["git", "rev-parse", "HEAD"], cwd=MAIN_DIR, capture_output=True,
                           check=True).stdout.decode()))
        f.write("> git status:\n")
        f.write(subprocess.run(["git", "status"], cwd=MAIN_DIR, capture_output=True, check=True).stdout.decode())
        f.write("\n> git diff:\n")
        # exclude refit-overrides as it is already included in each testcase
        f.write(subprocess.run(["git", "diff", ":!scripts/config/refit-overrides"],
                               cwd=MAIN_DIR, capture_output=True, check=True).stdout.decode())

    variant_file = os.path.join(genconfig_dir, "variants-list")
    if args.multi:
        print("Generating configuration variants")
        config_helper(["./test/helper/config-variants-generator.py", "-n", args.scenario, "echo", '{}'], variant_file,
                      scripts_dir)

        if args.check:
            subprocess.run(["./test/helper/config-variants-generator.py", args.scenario, "-d", MAIN_DIR,
                            "make", "check-config"], cwd=scripts_dir, check=True)
    else:
        if args.check:
            subprocess.run(["make", "check-config"], cwd=MAIN_DIR, check=True)
        with open(variant_file, "w") as f:
            f.write(args.scenario)

    variants = load_variants(variant_file)
    if variants is None:
        return False

    cmds = []
    results_path = "~/\"{}/{}\"".format(config["queue.results"], experiment_name)
    basepath = ""
    if args.parent_folder:
        basepath = os.path.join(os.path.basename(MAIN_DIR), "")

    cmds.append("mkdir -p {} && cp {}scripts/generated-config/git-info {}/git-info".format(results_path, basepath,
                                                                                           results_path))

    if args.multi:
        for variant in variants:
            cmds.append("cd {}scripts"
                        " && ./test/helper/config-variants-generator.py -s {}"
                        " -c ./generated-config/refit-overrides -d test"
                        " -- {} ./{}.py run -r {} {} '{{}}' {}"
                        .format(basepath, variant, args.scenario, args.testfile, results_path,
                                args.testfile, " ".join(args.extra)))
    else:
        cmds.append("cd {}scripts/test"
                    " && ./{}.py run -r {} {} {} {}"
                    .format(basepath, args.testfile, results_path, args.testfile, args.scenario,
                            " ".join(args.extra)))

    print("Running experiment {} times".format(len(cmds) - 1))

    if args.add_analyze:
        for step in args.add_analyze:
            cmd = "{}scripts/exp/exp -C {} analysis {} >> {}/analysis.log 2>&1".format(
                basepath, results_path, step, results_path)
            cmds += ["echo '$ ' " + cmd, cmd]

    with open(os.path.join(genconfig_dir, "commands"), "w") as f:
        f.write("\n".join(cmds))

    return True


def config_helper(cmd, output, scripts_dir):
    ret = subprocess.run(cmd, cwd=scripts_dir, check=True, stdout=subprocess.PIPE)
    with open(output, "wb") as f:
        f.write(ret.stdout)


def load_variants(variant_file: str) -> Optional[Sequence[str]]:
    variants = []
    with open(variant_file, "r") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            variants.append(line)

    counter = len(variants)
    if counter == 0:
        print("No config variants found")
        return None
    print("Found {} config variants".format(counter))

    if len(variants) != len(set(variants)):
        print("Non-unique variant names found")
        return None

    return variants


def queue(prio: bool, experiment_name: str, parent_folder: bool) -> bool:
    queue_flag = []
    if prio:
        queue_flag += ["--prio"]
    if parent_folder:
        queue_flag += ["--parent-folder"]
    subprocess.run(["scripts/queue/upload.py", experiment_name] + queue_flag, cwd=MAIN_DIR, check=True)
    return True


def poll_results(result_path):
    scripts_dir = os.path.join(MAIN_DIR, "scripts")
    queue_file = os.path.join(scripts_dir, "config", "queue")
    config = TestRun.load_config(queue_file)

    error_count = 0

    while True:
        try:
            subprocess.run(["rsync", "-av", "-F",
                            "{}@{}:{}/".format(config["queue.user"], config["queue.host"], config["queue.results"]),
                            "{}/results".format(result_path)],
                           check=True)
            error_count = 0
        except subprocess.CalledProcessError as e:
            error_count += 1
            if error_count > 3:
                raise e

        subprocess.call('read -t 60', shell=True)


def run(cls, args, exit_fn):
    with open(exit_fn, "w") as f:
        f.write("\n\n")

    args_dict = dict(vars(args))
    # remove standard args
    for key in ("cmd", "tmuxed", "resultsdir", "testfile", "scenario"):
        del args_dict[key]

    misc_args = [str(args_dict[key]) for key in sorted(args_dict.keys())]
    test_run_name = build_test_run_name(args.testfile, args.scenario, misc_args)
    results_dir = os.path.join(args.resultsdir, test_run_name)
    latest_dir = os.path.join(args.resultsdir, "latest")

    os.makedirs(results_dir, exist_ok=True)
    output_fn = os.path.join(results_dir, "test.log")
    redirect_output(output_fn)

    with open(exit_fn, "w") as f:
        f.write("\n" + output_fn + "\n")

    print("[PARAM] Test run name: {}".format(test_run_name))
    print("[PARAM] Results directory: {}".format(results_dir))
    print("[PARAM] Testfile: {}".format(args.testfile))
    print("[PARAM] Scenario: {}".format(args.scenario))
    for key in sorted(args_dict.keys()):
        value = args_dict[key]
        key = key.replace("_", " ")
        key = key[0].upper() + key[1:]
        print("[PARAM] {}: {}".format(key, value))

    test = cls(args.testfile, MAIN_DIR, results_dir, args)
    successful = test.run()

    # create / replace latest symlink
    if os.path.islink(latest_dir):
        # we don't expect race conditions, thus just check whether a symlink already exists
        os.unlink(latest_dir)
    os.symlink(os.path.relpath(results_dir, args.resultsdir), latest_dir, target_is_directory=True)

    print("====================== Complete =======================")
    return successful


def main(cls=TestRun):
    sys.exit(execute(cls, sys.argv))


if __name__ == '__main__':
    main()
