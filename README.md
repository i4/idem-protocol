# Idem: Preventing Overload-Induced Tail Latency via Proactive Rejection

This repository contains a prototype implementation of **Idem**, a crash fault-tolerant protocol specifically designed to process client requests with low latency even during load spikes.
Idem achieves this by using proactive rejection to avoid overload-induced tail latency and maintain a stable latency both in normal operation and severe overload situations.
Idem uses a collaborative approach for overload prevention, allowing it to provide rejects also during replica crashes.

For a detailed description of Idem and its overload prevention mechanism, please refer to the paper [Targeting Tail Latency in Replicated Systems with Proactive Rejection](fixme) by [Laura Lawniczak](https://sys.cs.fau.de/person/lawniczak) and [Tobias Distler](https://sys.cs.fau.de/person/distler) published at the [25th ACM/IFIP International Middleware Conference](fixme).

## Requirements

We suggest to use the provided Dockerfile to run a docker container with all the necessary requirements already installed and the Idem environment pre-configured. Please be aware that building the docker image may take a couple of minutes.

The following command first builds the docker image ``idem:latest`` (if not already present) and starts a docker container named ``idem`` with an interactive shell session. Setting environment variables for the user id (``LOCALUID``) and group id (``LOCALGID``) ensures that the docker user has access to the mounted volume later.

```
# Run from the root folder of this repository
LOCALUID=$(id -u) LOCALGID=$(id -g) docker compose run --rm --name idem idem-service
```

If present, please ignore the warning "*pull access denied for idem*"; this is default behavior for docker compose and simply signals that the image could not be found in the docker registry.

After starting the docker container, please refer to [the next section](#running-idem) for instructions on how to run the Idem prototype.

If you do not want to use docker, we recommend using a freshly installed [Ubuntu Jammy Jellyfish (22.04)](https://releases.ubuntu.com/22.04/) to avoid unwanted effects with already installed packages or custom configuration.
The Idem prototype and quick start script require the following packages:

- Java Development Kit (JDK) Version >= 11
- Python >= 3.5
- rsync
- tmux
- ssh
- moreutils
- gnuplot
- librsvg2-bin

Further steps on how to setup Idem for evaluation when not using the provided docker image can be found [here](#setup-without-docker).

## Running Idem

The following provides a short explanation of how to run Idem. If not otherwise stated, all commands are meant to be run from the root folder of this repository (which is also the default folder when starting the docker container).

### Quick Start

For an easy demonstration of Idem, this repository provides a `start.sh` script that automatically evaluates Idem under different load situations and displays the results in several graphs.


To start the script, simply run the following command:

```
./scripts/start.sh
```

Please note that the whole experiment runs around 10 minutes and **should not be interrupted!**

The script does not require any interaction, but you can monitor the progress of the experiment in a second terminal by connecting to the queue runner tmux session:

```
# Connect a second terminal from your local machine to the docker container
docker exec -it -u idem idem /bin/bash

# Run inside the docker container
tmux attach-session -t 0
```

The queue runner itself also starts a nested tmux session for each experiment run, including all involved servers (``server-[0,1,2]``) and the client process (``main``) as tmux windows.
It is easiest to navigate between the individual (nested) windows by typing ``Ctrl+b w`` to show a scrollable list and select the desired window with ``Enter``.
To exit the tmux session, you can detach from it by typing ``Ctrl+b d`` (or simply closing the terminal).

For further help with navigating tmux, we recommend this [cheatsheet](https://tmuxcheatsheet.com/).


After the script has finished, the results can be found in the ``queue_results`` folder (the repository folder is mounted to the docker container as volume). The script already produces four graphs to visualize the results. The graphs are available both as ``.svg`` and ``.pdf``:

  * ``graphs-average.pdf``/``graphs-median.pdf``: Show the average/median relation of throughput to latency.
  * ``graphs-latency.pdf``/``graphs-throughput.pdf``: Show the latency/throughput over time for each experiment run.

Additionally, the individual results can be found in the ``client-0.log`` (only requests statistics) and ``original.client-0.log`` (statistics for requests and rejects) files for each experiment run.

The experiment results created by ``start.sh`` have the following structure:
```
queue_results
└── <timestamp>-idem
    ├── <timestamp>-refit-idem-1-120
    │   ├── client-0.log
    │   ├── original.client-0.log
    │   └── ...
    ├── <timestamp>-refit-idem-10-120
    │   └── ...
    ├── graphs-average.png
    ├── graphs-latency.png
    ├── graphs-median.png
    ├── graphs-throughput.png
    ├── graphs-average.svg
    ├── ...
    └── rejects # Reject-only statistic
        ├── <timestamp>-refit-rejects-idem-1-120
        │   └── client-0.log
        ├── <timestamp>-refit-rejects-idem-10-120
        │   └── client-0.log
        └── ...
```

*Note: Please ignore the error message at the end of the `test.log` files:*

```
Timeout of 70 seconds exceeded - aborting...
exec_helper failed: [...]
```

*This is simply a result of the configured YCSB workload operation count exceeding the test run time (to ensure meaningful results on machines with different processing power) and the script therefore manually killing the benchmarking process.*

### Setup Without Docker

If you have installed all the required dependencies for the Idem prototype, you can immediately start exploring [running your own experiments](#running-your-own-experiments) and other options provided by the [framework](#further-information).

The provided [quick start](#quick-start) script ``start.sh``, however, requires some additional setup steps:

  1) Ensure that full path to the repository folder does not contain any whitespaces or special characters.
  2) Configure your system to allow a **passwordless** ssh connection to localhost.
  3) Update ``scripts/config/queue`` with your local user name.
  4) Run ``scripts/queue/queue_runner.py`` in your **home** folder. The runner will automatically create the required folders (``~/queue`` and ``~/runner``).

### Running Your Own Experiments

To run further experiments, you can also manually configure and run Idem. This works the same for the docker container and a manual deployment.

#### Configuration

To update the load of the system, change the number of clients in ``scripts/config/refit-overrides``. The larger the number, the higher the load of the system.

The ``scripts/config/refit-defaults`` file offers many additional configuration options. The options related to Idem are marked as `replica.idem.<option>` (e.g., the reject threshold ``replica.idem.reject_threshold``). To change a configuration option, simply add the entry and the desired value in the ``refit-overrides`` file.
Please note that adjusting the *non-Idem* configuration options may lead to a misconfiguration of the system and unexpected or undesired behavior, so proceed with caution.

#### Manual Running
To manually run an Idem test run, run the following command after adjusting the configuration:

```
./scripts/exp/exp run refit idem <runtime>
```

The results are then stored in the ``results`` folder.

### Further Information

Idem is implemented as part of the REFIT-framework. The framework offers a large variety of further benchmarking and analysis tools not included in this short description, both for local and distributed experiments.
For more information about the framework and a detailed documentation of its capabilities, please refer to the [REFIT-framework documentation](FRAMEWORK.md).

## License

Idem is being developed by the [Department of Computer Science 4 at Friedrich-Alexander-Universität Erlangen-Nürnberg](https://sys.cs.fau.de)

**Idem** Copyright @ 2024; Laura Lawniczak; Released under the [Apache License](LICENSE)

**REFIT-Framework** Copyright @ 2024; Laura Lawniczak, Michael Eischer; Released under the [Apache License](LICENSE)
