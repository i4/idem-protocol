# REFIT-Framework

The REFIT framework allows to implement various crash and Byzantine fault-tolerant state machine replication protocols and system architectures. It is written in the Java programming language.

The structure of this repository is as follows:

- `src`: The protocol implementation itself
- `lib`: External libraries
- `scripts/config`: The framework configuration
- `scripts/{analysis,exp,queue,test}`: Scripts to distribute and run the framework as well as to collect and analyze the
  experiment results
- `experiments`: Sample configurations for the framework

## Requirements

The framework requires the following packages:

- Java Development Kit (JDK) Version >= 11
- Python >= 3.5
- rsync
- tmux
- ssh
- moreutils

## Initial setup

The benchmark scripts automatically compile the source code using the Makefile. To build the code manually run `make`
or `make test`, where the latter also compiles the unit tests.

The experiment framework needs to know on which hosts the experiments should be run. For this we will use the helper
script `exp` which serves as the central command from which it is possible to configure the used servers, start
experiments, retrieve results and run analysis scripts. After calling `exp shell`, which starts a subshell, the `exp`
command can be called from within any folder while the commands effects are bound to the current repository.
Calling `exp` without further parameters prints a list of available commands.

```
> scripts/exp/exp shell
> exp servers local
```

The `servers` subcommand creates a symlink in `scripts/config` which tells the framework which server configuration to
use. A plain call of `exp servers` prints the currently active server configuration along with a list of other available
configurations. The `local` server configuration start all servers and clients on the local host.

Before the first run it is also necessary to generate the asymmetric keys used in certain configurations by
calling `make keys`:

```
> make keys
[...]
mkdir -p scripts/keys
java -cp bin/java:lib/eddsa-0.3.0.jar:lib/sqlite-jdbc-3.23.1.jar refit.crypto.REFITKeyManager
Generating 504 keys
Generating 504 keys
[...]
```

## Benchmarking

`> exp run refit example 20`

This starts a _local_ test run using the refit script (`scripts/test/refit.py`) with a duration of _20 seconds_. The
number of clients, replicas, their location, the actual replication protocol and system architecture are automatically
determined from the configuration files `scripts/config/refit-{defaults,overrides}`. The latter file is used to only
override configuration options relevant for the experiment setting in order to keep the configuration compact. The
parameter `example` is the scenario name and is added to the result folder name.

The experiment script automatically opens a tmux sessions with separate windows for each client and replica. These show
a live output of the experiment progress, which is also saved to a folder in `results`.

The client (in the first tmux window) will after the initial connection setup print one line with average throughput and
latency for the last second. The values in brackets are the minimal and maximal latency.

`> cat results/2020_06_30-14_45_23-refit-example-20/test.log`

```
[PARAM] Test run name: 2020_06_30-14_45_23-refit-example-20
[PARAM] Results directory: results/2020_06_30-14_45_23-refit-example-20
[PARAM] Testfile: refit
[PARAM] Scenario: example
[PARAM] Duration: 20
[BUILD] Building class files
make: Nothing to be done for `all'.
[BENCH] Starting server-0
[BENCH] Starting server-1
[BENCH] Starting server-2
[BENCH] Starting server-3
Number of clients: 100
ClientID offset: 4
1593521124530085 main [EVENT] BENCH: Startup delay: 0.77900004 s
[...]
1593521125294750 BENCH5 [EVENT] BENCH: OK
1593521125296458 BENCH5 [EVENT] BENCH: Start time: 1,552s
1593521125599493    1    264 163994 (  8890/813944)
1593521126608168    2   1515 103532 ( 14848/1245969)
1593521127599082    3   2150  45807 ( 18890/ 93178)
1593521128600094    4   2508  40255 (  9992/ 94144)
1593521129599925    5   2706  36658 (  8285/ 94732)
1593521130600066    6   4244  23910 (  7936/ 64995)
1593521131599183    7   4806  20773 (  7420/ 40461)
1593521132599057    8   6419  15645 (  7278/ 40885)
1593521133599279    9   8093  12400 (  6257/ 26569)
1593521134600176   10   8683  11445 (  5671/ 26666)
1593521135599403   11   8157  12306 (  6063/ 29231)
1593521136599369   12   6997  14249 (  5917/ 34595)
1593521137600348   13   8955  11221 (  4154/ 46973)
1593521138600990   14   7247  13808 (  5812/ 31575)
1593521139601636   15   8153  12224 (  6776/ 20913)
1593521140598308   16   9230  10850 (  5869/ 18810)
1593521141599036   17   9163  10890 (  5268/ 18486)
1593521142599221   18   8797  11369 (  5021/ 22746)
1593521143601304   19   9428  10642 (  5822/ 19012)
1593521144600497   20   9439  10587 (  5623/ 17408)
1593521144601042 main [EVENT] BENCH: END: 126954   6348  15760 ( 20)
=== Warmup histogram ===
Percentile 0%: 5,664000 ms
Percentile 25%: 11,775000 ms
Percentile 50%: 15,295000 ms
Percentile 75%: 26,367000 ms
Percentile 99%: 84,479000 ms
Percentile 100%: 1253,375000 ms
=== Histogram ===
Percentile 0%: 4,128000 ms
Percentile 25%: 9,855000 ms
Percentile 50%: 11,135000 ms
Percentile 75%: 12,927000 ms
Percentile 99%: 21,887000 ms
Percentile 100%: 47,103000 ms
=== Client progress ===
1105 - 1163: 14
1171 - 1228: 16
1235 - 1297: 24
1299 - 1361: 30
1370 - 1420: 16
Main client finished with return code 0
[BENCH] Waiting for clients
[BENCH] Closing screens
====================== Complete =======================
```

The servers are silent during regular operation and just print the execution progress every few thousand executed
sequence numbers.

```
1593521124679894 main [EVENT] RPLCA: READY
1593521124742943 RPLC3-0 [EVENT] ORDER: switch to REFITPBFTProtocol (view 0)
1593521124743660 RPLC3-0 [EVENT] ORDER: 0 is now the contact replica for group 0
1593521124757904 RPLC3-0 [EVENT] CLINT[0]: Configuration update (send replies: true contactReplica: 0)
1593521124782537 RPLC3-0 [EVENT] EXCTR: change checkpoint-creation setting to "regular"
1593521124782948 RPLC3-0 [EVENT] EXCTR: change update-creation setting to "disabled"
1593521125306946 RPLC3-0 [EVENT] EXCTR: Start time: 1,562s
1593521125307228 RPLC3-0 [EVENT] EXCTR:          0 @ 1593521125307092
1593521129028089 RPLC3-0 [EVENT] EXCTR:       1000 @ 1593521129027782
1593521131059981 RPLC3-0 [EVENT] EXCTR:       2000 @ 1593521131059783
1593521132405974 RPLC3-0 [EVENT] EXCTR:       3000 @ 1593521132405644
1593521133409633 RPLC3-0 [EVENT] EXCTR:       4000 @ 1593521133409460
1593521134302389 RPLC3-0 [EVENT] EXCTR:       5000 @ 1593521134302278
1593521135241370 RPLC3-0 [EVENT] EXCTR:       6000 @ 1593521135241204
1593521136316225 RPLC3-0 [EVENT] EXCTR:       7000 @ 1593521136316077
1593521137220909 RPLC3-0 [EVENT] EXCTR:       8000 @ 1593521137220707
1593521138263026 RPLC3-0 [EVENT] EXCTR:       9000 @ 1593521138262870
1593521139214084 RPLC3-0 [EVENT] EXCTR:      10000 @ 1593521139213940
1593521140143988 RPLC3-0 [EVENT] EXCTR:      11000 @ 1593521140143865
1593521140998286 RPLC3-0 [EVENT] EXCTR:      12000 @ 1593521140998166
1593521141922088 RPLC3-0 [EVENT] EXCTR:      13000 @ 1593521141921903
1593521142789127 RPLC3-0 [EVENT] EXCTR:      14000 @ 1593521142788968
1593521143625455 RPLC3-0 [EVENT] EXCTR:      15000 @ 1593521143625284
1593521144471547 RPLC3-0 [EVENT] EXCTR:      16000 @ 1593521144471374
```

The log output format of both clients and replicas always starts with a timestamp in microseconds which is followed by
the thread name (e.g. `RPLC1-0` is thread 0 on replica 1), the log event type, the component that created the log output
and the output itself.

## Distributed execution

As a first step create a new `scripts/config/servers-*` file, for example `servers-cloud`, and then activate it using
`exp servers <name>`, for example `exp servers cloud`. Subsequent calls to `exp run` will use this server configuration.

The server configuration file is used to resolve placeholders like `client0` configured in `replica.network.addresses`
and `client.network.addresses` of `refit-defaults` or `refit-overrides`. Note that although the current configuration
only uses placeholders like `client0` or `server0` it is possible to use arbitrary names like `s-euw1`. Each host entry
must as a minimum contain a `client0 = <external ip>` entry with an IP that is directly reachable from all used hosts (
via ssh). By adding a suffix it is possible to specify additional parameters for a host such as `loc` for the location
id (a zero-based counter, which should be identical for all hosts in the same region) and `int` to specify the internal
IP of a host which is accessible for other hosts in the same region.

```
client0 = <external IP>
client0loc = 0
client0int = <internal IP>
```

The benchmarking scripts require a password-less login via SSH to each server. In addition, the server configuration
file must also contain the user name for the remote servers and a path to where the framework repository should be
copied on the server.

```
remote.user = username

# {} expands to the remote.user name; paths starting with ~/ are interpreted
# relative to the users home directory no futher path variable expansion will
# take place!
remote.path = remote-runner
```

The `terraform` folder contains configuration files and further instructions on how to setup servers for measurements in
EC2.

## Variants generator and execution queue

The config files use an ini-like format containing `key = value` assignments. Values spanning multiple lines must use
trailing backslashes ` \ ` to mark line continuations. The `REFITConfig` class checks that each setting
in `refit-overrides` actually overrides one in `refit-defaults` and that there are no unused settings.

The experiment framework provides two features to simplify the evaluation of configuration variants. The base component
is the experiment queue (`scripts/queue`). It continuously checks a queue folder for new experiments and once a new one
is found, unpacks it and runs each command from an accompanying list. Building on that, the variants generator provides
a simple way to generate and run configuration variants, that run the experiment using different values for a setting
and are able to handle basic conflicts and requirements between different settings.

The experiment queue consists of the `queue_runner.py` which should be started on a server (preferably in tmux/screen)
and which continuously checks the `queue/normal` and `queue/prio` folders for queued experiments. The execution of an
experiment `<experiment-name>.tgz` works by unpacking the archive of the repository to the `runner` folder and executing
the commands listed in `<experiment-name>.tgz.commands` (one per line) in that folder. To ensure that the execution of
an experiments waits until the upload is complete, the queue requires the creation of an empty
`<experiment-name>.tgz.marker` file. To simplify aborting failing experiments, the queue waits for one seconds between
failed experiments (according to their exit code). Press 'Ctrl-C' during this time to abort the execution of the current
experiment.

The `upload.py` script uploads the current state of the local repository to the server and folder configured
in `scripts/config/queue`. The command list is taken from `scripts/generated-config/commands`. The upload script is
usually not called manually but rather implicitly by `exp remote ...` (see below).

Configuration variants must be specified in the `refit-overrides` configuration file. An variants configurations
for `application.request_size` could look as follows:

```
application.request_size.variants = \
    Value("1024", "data1024") \
    Value("4096", "data4096")
```

The setting's key must be suffixed with `.variants` followed by a list of values for this key. The line containing the
key must not contain a value, as shown in the example. Each `Value(value[, name])` contains a value for a setting as
first parameter and a name as optional second parameter. The value name gets appended to the scenario name and can be
referred to as requirement `requires` or conflict `conflicts` by later variant settings:

```
application.reply_size.variants = \
    Value("1024", requires=("data1024",)) \
    Value("4096", requires=("data4096",))
```

`requires` must be followed by a list of value names that are the prerequisite for this Value. `conflicts` matches when
any of the listed value names is part of the scenario name. The list of value names with a single entry must be written
as `("nameA",)` whereas the trailing comma is optional with multiple entries `("nameA", "nameB")`.

The variant settings are evaluated from first to last in a depth-first manner. That is the order of variant entries is
relevant (the ini file format allows you to use whatever order of settings necessary), and the last option usually
changes after between consecutive experiment variants whereas the first option only changes to each value once.

A single experiment run with the current configuration (i.e. ignoring the variants settings) can be queued by
calling `exp remote refit example 20`. To queue the configuration variants of an experiment
call `exp remote --multi refit example 20`. The script will run `scripts/test/helper/config-variants-generator.py` to
validate the config file syntax and generate a list of variants which is then used to create the command list for the
experiment. The results of an experiment run queued via `exp remote` are stored on the server running the queue in the
directory specified by `queue.results`.

These experiments can be synced to a local folder via `exp result <foldername>`. The script polls for new results every
60 seconds or immediately after pressing 'return'.

## Reproducible experiments

The results folder for an experiment run contains the configuration files that were used by that experiment run. To
repeat the execution of a specific experiment configuration for futher analysis or debugging,
`exp apply-config <result-dir>` applies the configuration of the experiment run to `scripts/config` after creating a
backup of the current configuration. To revert to the previous configuration run `exp apply-config --revert`.

The repository contains a set of predefined experiments in the `experiments` folder. Each experiment contains a `run.sh`
script that temporarily copies the configuration to `scripts/config`, enqueues the experiment using the
current `servers` configuration and reverts the temporary changes. The script can either be called directly via
`./run.sh` or by calling `exp experiment <experimentA> <experimentB> ...`. Note that an experiment configuration takes
no further parameters to enforce that all settings are stored in the configuration.

## Analysis scripts

`scripts/analysis` contains several scripts to help with the analysis of the generated log files. `parse-logs.py` is the
main analysis script and should be called from a folder which contains the results of the experiment runs that should be
analysed. It automatically checks the logfile lengths and suggests which timespan of the experiments runs should be used
to avoid warmup / shutdown effects. For a proper analysis it is necessary to select the exact timespan via
the `--range <from> <to>` parameter. The log parsing script currently only handles log files for experiment runs of the
`refit` script, whose results folders also contain the word `refit` at the start of their scenario part.

In a nutshell the script works as follows: First it uses the parsers in `scripts/analysis/parser` to extract useful data
from various log files. Each parser can specify which types of logfiles it is able to process. The extracted data
consists of a timestamp in microseconds (which the logfile should contain) and either a warning message or log type
dependent data fields. The analysis script assumes a reasonable clock synchronization (using NTP) on all servers, that
is clocks that diverge only by a few milliseconds from each other.

The analysis script then converts the timestamps into relative durations since the start of the experiment and checks
the log files for warnings. The logfiles of the client and servers are checked against a whitelist by
`scripts/analysis/transformer/{client,server.py}`. Afterwards the 10 first remaining warnings per logfile are printed.

The next step is to group the results by their scenario and calculate the average throughput and several latency
percentiles. Then the results are trimmed to the configured time range and are finally output in the current folder.

A fine-grained request latency analysis requires that the `client.statistics.latency` option is set to true.

The `preview-results.py` generates preview plots of the analysis results.

## Experiment script details

The standard execution steps of an experiment script in `scripts/test` are as follows:
_repository_ refers to the current folder which also includes the README file and if appropriate all contained files and
folders.

1. Redirect the console output to a `test.log` file for the current experiment run in the `results` directory.
2. Build the framework locally by running `make` in the _repository_. This also copies the configuration files listed
   in `self.config_files` and `self.extra_files` of the experiment script to the `results` folder.
3. Read the hostnames of the servers used in the experiment from the configuration files (
   i.e. `refit-{defaults,overrides}`) by resolving all hostnames contained in the `DISTRIBUTE_TO_TYPES` variable of the
   experiment script. The usual behavior is to use hosts listed in `replica.network.addresses` and
   `client.network.addresses`. Then _rsync_ will copy the repository except for a few excluded folder (e.g. `.git`,
   `terraform`) to the configured hosts.
4. A benchmark usually starts multiple remote processes using the `scripts/test/helper/exec_helper` script which ensures
   that the command is stopped/killed after a given timeout, runs the command in the repository folder on the remote
   host and sets the `OUTPUT_DIRECTORY` and `OUTPUT_ID` environment variables. The directory specified by the former
   variable is automatically collected once the benchmark has completed. The helper script also automatically logs
   CPU/RAM usage, and network usage (only on EC2). The `refit` benchmark script first starts all replicas, then all
   clients, waits until the main client exits and stop all other processes after a short delay. The options to run java
   are read from `scripts/config/java`.
5. The log files from each hosts' `OUTPUT_DIRECTORY` are collected using `rsync`. At this point the files from each host
   are in a subfolder whose name is given by `OUTPUT_ID`. In case there are no colliding filenames, then the experiment
   will flatten the results folder.

## Debugging help

To debug problems with an experiment run, take a look at log files in the results folder, especially `test.log` and
the `client*.log` and `server*.log` files.

Useful configuration options:

- `system.debug_checks` should always be set to true to enable several sanity checks
- `system.trace_messages` captures a stacktrace whenever a `REFITMessage` is created and thus provides information on
  the source of a message that caused an exception.
- `system.track_scheduler_hangs` prints a warning if a scheduler run which executes the actors (`REFITStage`
  and `REFITSchedulerTask`) takes longer than half a second. A hung scheduler prevents actors from receiving new
  messages from the network.

For debugging it is possible to run all clients and replicas in a single process by running `REFITLocalSystem` which
expects a test run duration in seconds as first parameter. The access the state of all replicas pause the process in a
debugger, select the "main" thread and choose the stack frame pointing to REFITLocalSystem. There all replicas are
available via the "replicas" array. 
