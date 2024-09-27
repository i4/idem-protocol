#!/bin/bash

BASEDIR=$(pwd "$0")
cd $BASEDIR

# Create symbolic link to store results in the repository
RESULTS_FOLDER=~/queue/results
if [[ -d $RESULTS_FOLDER ]]; then
    rm -rf $RESULTS_FOLDER
fi
ln -s $BASEDIR/queue_results $RESULTS_FOLDER

# Setup experiment environment
./scripts/exp/exp servers local

# Start experiment
echo "Starting experiment"
./scripts/exp/exp experiment idem

echo -e "Sleeping until experiment is finished...\nThis will take around 10 minutes!\n\nYou can monitor the progress in a second terminal with the queue runner tmux session via:\n> tmux attach-session -t 0\n\nIf the script is run within a docker container, you first need to connect to the container, for example via:\n> docker exec -it -u idem idem /bin/bash\n\n\033[1mDo not interrupt this script!\033[0m\n"

SECONDS=0
RUNNER_FOLDER=~/runner
echo -n "Starting..."
state=( "|" "/" "-" "\\" )
# Sleep until queue runner has started the experiment
for (( i = 0 ; ; i = (i + 1) % ${#state[@]} )) ; do
	echo -en "\r${state[$i]} Processing ($SECONDS s)"
	if [[ -d "${RUNNER_FOLDER}" ]] ; then
		break
	else
		sleep 0.3
	fi
done
# Sleep until queue runner is finished
for (( i = 0 ; ; i = (i + 1) % ${#state[@]} )) ; do
	echo -en "\r${state[$i]} Processing ($SECONDS s)"
	if [[ ! -d "${RUNNER_FOLDER}" ]] ; then
		echo -e "\rExperiment finished! Processing results..."
		break
	else
		sleep 0.3
	fi
done

# Use latest results folder
cd $RESULTS_FOLDER
cd `ls -Art | tail -n 1`
# Cleanup files
rm -f */*.ycsb
# Separate reject information
$BASEDIR/scripts/analysis/investigation/process-split-rejects.py
if [[ -d rejects ]]; then
    rm -rf rejects
fi
mkdir rejects
mv *-rejects-* rejects
# Parse logs
$BASEDIR/scripts/analysis/parse-logs.py
$BASEDIR/scripts/analysis/preview-results.py
# Convert resulting graphs to png
rsvg-convert --format=pdf graphs-average.svg -o graphs-average.pdf
rsvg-convert --format=pdf graphs-median.svg -o graphs-median.pdf
rsvg-convert --format=pdf graphs-latency.svg -o graphs-latency.pdf
rsvg-convert --format=pdf graphs-throughput.svg -o graphs-throughput.pdf