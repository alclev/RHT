#!/bin/bash

# cl.sh
#
# A script for uploading and running romulus applications on the Sunlab

set -e # Halt the script on any error

# Print usage information
usage() {
	cat <<EOF

cl.sh — tool for building & running binaries on the Sunlab
...
EOF
}

# SSH into MACHINES once, to fix known_hosts
function cl_first_connect {
	echo "Performing one-time connection to Sunlab MACHINES, to get known_hosts right"
	for machine in ${MACHINES[@]}; do
		echo "$USER@$machine.$DOMAIN"
		ssh -o StrictHostKeyChecking=no $USER@$machine.$DOMAIN echo "Connected"
	done
}

# Append the default configuration of a screenrc to the given file
function make_screen {
	echo 'startup_message off' >>$1
	echo 'defscrollback 10000' >>$1
	echo 'autodetach on' >>$1
	echo 'escape ^jj' >>$1
	echo 'defflow off' >>$1
	echo 'hardstatus alwayslastline "%w"' >>$1
}

# Check the status of IBV on the target MACHINES
function check_ibv {
	echo "Checking ibv status:"
	for machine in ${MACHINES[@]}; do
		echo "$machine:"
		ssh $USER@$machine.$DOMAIN "ibv_devinfo -v"
	done
}

#  Configure the set of remote MACHINES
function cl_install_deps() {
	echo "Setup initial connection"
	ssh-add "$HOME/.keys/sunlab"
	cl_first_connect
	create_env
}

# Function to send hostnames of all **other** nodes to each node
function create_env() {
	# entire sunlab
	MACHINES=("ariel" "caliban" "callisto" "ceres" "chiron" "cupid" "eris" "europa" "hydra" "iapetus" "io" "mars" "mercury" "neptune" "nereid" "nix" "orcus" "phobos" "puck" "saturn" "triton" "varda" "vesta" "xena")
	outfile="$HOME/sunlab.env"
	rm -rf $outfile
	touch $outfile
	global_id=0
	for m in ${MACHINES[*]}; do
		echo -e "${global_id} ${m}.${DOMAIN}" >>${outfile}
		global_id=$((global_id + 1))
	done
}

# SEND and RUN a binary on the remote MACHINES
# $1 : Relative path of exe
function cl_run() {
	# check if file exists
	EXE_NAME=$(basename "$1")
	if [[ ! -f "build/$1" ]]; then
		echo "Executable not found: $1"
		exit 1
	fi
	for m in ${MACHINES[*]}; do
		scp "build/$1" "${USER}@${m}.${DOMAIN}:${EXE_NAME}" &
	done
	wait
	rm -rf logs
	mkdir logs
	# Set up a screen script for running the program on all MACHINES
	tmp_screen="$(mktemp)" || exit 1
	make_screen "$tmp_screen"
	NUM_MACHINES=${#MACHINES[@]}
	# Populate node list
	NODE_LST=""
	for i in "${!MACHINES[@]}"; do
		host="${MACHINES[$i]}"
		global_id=$(grep "$host" ~/sunlab.env | awk '{print $1}')
		if [[ -n "$NODE_LST" ]]; then
			NODE_LST+=","
		fi
		NODE_LST+="$global_id"
	done

	for i in "${!MACHINES[@]}"; do
		host="${MACHINES[$i]}"
		global_id=$(grep "$host" ~/sunlab.env | awk '{print $1}')
		ARGS="${i} ${global_id} ${NODE_LST} ${NUM_MACHINES} -p ${TCP_PORT} -t ${NUM_THREADS} -k ${KEY_RANGE} -r ${REP_DEGREE}"
		# gdb -ex \"r\" --args
		CMD="./${EXE_NAME} ${ARGS}"
		echo "$CMD"
		cat >>"$tmp_screen" <<EOF
screen -t node${i} ssh ${USER}@${host}.${DOMAIN} ${CMD}
logfile logs/log_${i}.txt
log on
EOF
	done

	screen -c "$tmp_screen"
	rm "$tmp_screen"
}

# $1 : Relative path of exe
function cl_debug() {
	EXE_NAME=$(basename "$1")
	if [[ ! -f "build/$1" ]]; then
		echo "Executable not found: $1"
		exit 1
	fi
	# Send the executable to all MACHINES
	for m in ${MACHINES[*]}; do
		scp "build/$1" "${USER}@${m}.${DOMAIN}:${EXE_NAME}" &
	done
	wait
	rm -rf gdb-logs
	mkdir gdb-logs

	# Set up a screen script for running the program on all MACHINES
	tmp_screen="$(mktemp)" || exit 1
	make_screen $tmp_screen

	gdb_cmd="$2"
	echo "Running gdb with command: $gdb_cmd"

	for i in "${!MACHINES[@]}"; do
		host="${MACHINES[$i]}"
		CMD="--hostname ${host} --node-id ${i} --leader-fixed ${ARGS}"
		if [[ $i -eq 0 && -n "$gdb_cmd" ]]; then
			cat >>"$tmp_screen" <<EOF
screen -t node${i} ssh ${USER}@${host}.${DOMAIN} gdb -ex \"${gdb_cmd}\" -ex \"r\" --args ./${EXE_NAME} ${CMD}; bash
logfile gdb-logs/gdb_${i}.log
log on
EOF
		else
			cat >>"$tmp_screen" <<EOF
screen -t node${i} ssh ${USER}@${host}.${DOMAIN} gdb -ex \"r\" --args ./${EXE_NAME} ${CMD}; bash
logfile gdb-logs/gdb_${i}.log
log on
EOF
		fi
	done
	screen -c $tmp_screen
	rm $tmp_screen
}

# Connect to remote nodes (e.g., for debugging)
function cl_connect() {
	last_valid_index=$((${#MACHINES[@]} - 1)) # The 0-indexed number of nodes

	# Set up a screen script for connecting
	tmp_screen="$(mktemp)" || exit 1
	make_screen $tmp_screen
	for i in $(seq 0 ${last_valid_index}); do
		echo "screen -t node${i} ssh ${USER}@${MACHINES[$i]}.${DOMAIN}" >>${tmp_screen}
	done
	screen -c $tmp_screen
	rm $tmp_screen
}

function do_all {
	for i in "${!MACHINES[@]}"; do
		ssh ${USER}@${MACHINES[$i]}.${DOMAIN} "$1" &
	done
	wait
}

function reset-all() {
	last_valid_index=$((${#MACHINES[@]} - 1)) # The 0-indexed number of nodes
	for i in $(seq 0 ${last_valid_index}); do
		ssh ${USER}@${MACHINES[$i]}.${DOMAIN} "killall -9 -u abc324" &
	done
	wait
	echo "Nodes have been reset."
}

function reset() {
	last_valid_index=$((${#MACHINES[@]} - 1)) # The 0-indexed number of nodes
	for i in $(seq 0 ${last_valid_index}); do
		ssh ${USER}@${MACHINES[$i]}.${DOMAIN} "pkill -f '^./${EXE_NAME}'" &
	done
	wait
	echo "Nodes have been reset."
}

# Get the important stuff out of the command-line args
cmd=$1   # The requested command
count=$# # The number of command-line args
# Navigate the the project root directory
cd $(git rev-parse --show-toplevel)
# Load the config right away
# source config/cloudlab.conf
source config/remotes.conf
# Additional config
NUM_THREADS=8
KEY_RANGE=100000

if [[ "$cmd" == "install-deps" && "$count" -eq 1 ]]; then
	cl_install_deps
elif [[ "$cmd" == "build-run" && "$count" -eq 3 ]]; then
	if [[ "$2" != "debug" && "$2" != "release" ]]; then
		usage
		exit 1
	fi
	sudo docker run -e MODE="$2" --privileged --rm -v $(pwd):/root --name mu -it rht:latest
	# if [[ "$2" == "debug" ]]; then
	# 	make DEBUG=1
	# else
	# 	make
	# fi
	cl_run "$3"
elif [[ "$cmd" == "run" && "$count" -eq 2 ]]; then
	cl_run "$2"
elif [[ "$cmd" == "connect" && "$count" -eq 1 ]]; then
	cl_connect
elif [[ "$cmd" == "reset" && "$count" -eq 2 ]]; then
	reset $2
elif [[ "$cmd" == "reset-all" && "$count" -eq 1 ]]; then
	reset-all
elif [[ "$cmd" == "do-all" && "$count" -eq 2 ]]; then
	do_all "$2"
elif [[ "$cmd" == "find-machines" && "$count" -eq 1 ]]; then
	# entire sunlab
	MACHINES=("ariel" "caliban" "callisto" "ceres" "chiron" "cupid" "eris" "europa" "hydra" "iapetus" "io" "mars" "mercury" "neptune" "nereid" "nix" "orcus" "phobos" "puck" "saturn" "triton" "varda" "vesta" "xena")
	do_all "echo -e \"\$(hostname)\n\"; ss -tuln | grep ${TCP_PORT}"
elif [[ "$cmd" == "run-experiment" && "$count" -eq 2 ]]; then
	mkdir -p results
	echo "Throughput,AvgLatency,NumThreads,RepDegree,KeyRange,SystemSize" >results/exp_1.csv
	REP_DEGREE=1
	# Experiment 1: Vary the key range
	echo "Starting experiment #1..."
	for rep in {1,2,3}; do
		REP_DEGREE=$rep
		for range in {10000,100000,1000000,10000000}; do
			KEY_RANGE=$range
			echo "Launching experiment with ${KEY_RANGE} key range..."
			cl_run "$2"
			# generate tmp filename with rand 4 chars
			tmp_file=$(mktemp)
			scp ${USER}@${MACHINES[0]}.${DOMAIN}:metrics.csv $tmp_file
			# append the second line of tmp file to exp_1.csv
			tail -n 1 $tmp_file >>results/exp_1.csv
			do_all "lsof -ti :${TCP_PORT} | xargs kill -9"
			rm $tmp_file
		done
	done
	echo "Throughput,AvgLatency,NumThreads,RepDegree,KeyRange,SystemSize" >results/exp_2.csv
	# Experiment 2: Vary the number of nodes
	KEY_RANGE=1000000
	NUM_THREADS=8
	REP_DEGREE=1
	echo "Starting experiment #2..."
	ORIG_MACHINES=("${MACHINES[@]}")
	for i in $(seq 2 ${#ORIG_MACHINES[@]}); do
		MACHINES=("${ORIG_MACHINES[@]:0:$i}")
		echo "Launching experiment with ${#MACHINES[@]} nodes..."
		cl_run "$2"
		tmp_file=$(mktemp)
		scp ${USER}@${MACHINES[0]}.${DOMAIN}:metrics.csv $tmp_file
		tail -n 1 $tmp_file >>results/exp_2.csv
		do_all "lsof -ti :${TCP_PORT} | xargs kill -9"
		rm $tmp_file
	done
	echo "Throughput,AvgLatency,NumThreads,RepDegree,KeyRange,SystemSize" >results/exp_3.csv
	# Experiment 3: Vary the number of threads
	KEY_RANGE=1000000
	NUM_THREADS=8
	REP_DEGREE=1
	echo "Starting experiment #3..."
	for n in $(seq 1 12); do
		NUM_THREADS=$n
		echo "Launching experiment with ${NUM_THREADS} threads..."
		cl_run "$2"
		tmp_file=$(mktemp)
		scp ${USER}@${MACHINES[0]}.${DOMAIN}:metrics.csv $tmp_file
		tail -n 1 $tmp_file >>results/exp_3.csv
		do_all "lsof -ti :${TCP_PORT} | xargs kill -9"
		rm $tmp_file
	done
	echo "Throughput,AvgLatency,NumThreads,RepDegree,KeyRange,SystemSize" >results/exp_4.csv
	# Experiment 4: Vary the replication degree
	echo "Starting experiment #4..."
	NUM_THREADS=8
	for n in $(seq 1 5); do
		REP_DEGREE=$n
		echo "Launching experiment with replication degree ${REP_DEGREE}..."
		cl_run "$2"
		tmp_file=$(mktemp)
		scp ${USER}@${MACHINES[0]}.${DOMAIN}:metrics.csv $tmp_file
		tail -n 1 $tmp_file >>results/exp_4.csv
		do_all "lsof -ti :${TCP_PORT} | xargs kill -9"
		rm $tmp_file
	done

else
	usage
fi
