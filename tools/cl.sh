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

function wait_for_pattern() {
	local pattern="$1"
	local file="$2"

	# Wait until the file exists
	while [[ ! -f "$file" ]]; do
		sleep 0.1
	done

	# Poll the file until pattern appears
	while ! grep -qF "$pattern" "$file" 2>/dev/null; do
		sleep 0.1
	done
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

# SEND and RUN a binary on the remote MACHINES
# $1 : Relative path of exe
function cl_custom_run() {
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
		CMD="${CUSTOM_ARGS} ./${EXE_NAME} ${ARGS}"
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
	REP_DEGREE=2
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
elif [[ "$cmd" == "run-custom" && "$count" -eq 2 ]]; then
	CUSTOM_ARGS="gdb -ex \"catch throw\" -ex \"r\" --args"
	cl_custom_run "$2"
elif [[ "$cmd" == "find-machines" && "$count" -eq 1 ]]; then
	# entire sunlab
	MACHINES=("ariel" "caliban" "callisto" "ceres" "chiron" "cupid" "eris" "europa" "hydra" "iapetus" "io" "mars" "mercury" "neptune" "nereid" "nix" "orcus" "phobos" "puck" "saturn" "triton" "varda" "vesta" "xena")
	do_all "echo -e \"\$(hostname)\n\"; ss -tuln | grep ${TCP_PORT}"
elif [[ "$cmd" == "run-experiment" && "$count" -eq 2 ]]; then
	mkdir -p results

	# Experiment 1: Vary the key range and rep degree
	# echo "Starting experiment #1..."
	# echo "system_size,replication_degree,key_range,lat_us_avg,lat_us_p50,lat_us_p90,lat_us_p99,election_lat,thru_avg_ops_s" >results/exp_1.csv

	# for rep in {1,2,3,4,5}; do
	# 	echo "Resetting..."
	# 	do_all "kill -9 -1"

	# 	REP_DEGREE=$rep
	# 	for range in {1000,2000,4000,8000,16000,32000,64000,128000}; do
	# 		KEY_RANGE=$range
	# 		echo "Launching experiment with ${KEY_RANGE} key range..."
	# 		cl_run "$2"
	# 		grep -oP '\[PARSE\] \K.*' logs/log_0.txt >>results/exp_1.csv

	# 	done
	# done

	# Experiment 2: Vary the number of nodes in the system
	# echo "system_size,replication_degree,key_range,lat_us_avg,lat_us_p50,lat_us_p90,lat_us_p99,election_lat,thru_avg_ops_s" >results/exp_2.csv
	# KEY_RANGE=1000000
	# NUM_THREADS=8
	# REP_DEGREE=1
	# echo "Starting experiment #2..."
	# ORIG_MACHINES=("${MACHINES[@]}")
	# for i in $(seq 3 ${#ORIG_MACHINES[@]}); do
	# 	MACHINES=("${ORIG_MACHINES[@]:0:$i}")
	# 	echo "Launching experiment with ${#MACHINES[@]} nodes..."
	# 	cl_run "$2"
	# 	grep -oP '\[PARSE\] \K.*' logs/log_0.txt >>results/exp_2.csv
	# done

	# echo "Throughput,AvgLatency,NumThreads,RepDegree,KeyRange,SystemSize" >results/exp_3.csv
	# Experiment 3: Failover experiment
	echo "lat_us" >results/failover.csv
	KEY_RANGE=1000000
	NUM_THREADS=8
	REP_DEGREE=1
	echo "Starting experiment #3..."

	ITERATION=0
	NUM_ITERATIONS=500

	while [[ $ITERATION -lt $NUM_ITERATIONS ]]; do
		echo "Starting iteration ${ITERATION}..."
		ITERATION=$((ITERATION + 1))

		# check if file exists
		EXE_NAME=$(basename "$2")
		if [[ ! -f "build/$2" ]]; then
			echo "Executable not found: $2"
			exit 1
		fi
		# for m in ${MACHINES[*]}; do
		# 	scp "build/$2" "${USER}@${m}.${DOMAIN}:${EXE_NAME}" &
		# done
		# wait
		rm -rf logs
		mkdir logs

		(
			wait_for_pattern "[LEADER ELECTION] System stable." logs/log_0.txt
			echo "Pattern found. Killing leader..."
			# sleep 5
			# kill the leader
			ssh ${USER}@${MACHINES[0]}.${DOMAIN} "pkill -9 -f '${EXE_NAME}.*'"

			wait_for_pattern "[FAILOVER]" logs/log_1.txt
			echo "Pattern found. Extracting failover time..."

			RESULT=$(grep -oP '\[FAILOVER\] \K[\d.]+' logs/log_1.txt || true)
			if [[ -n "$RESULT" && "$RESULT" != "0" ]]; then
				echo "$RESULT" >>results/failover.csv
				echo "Failover time: $RESULT us"
			else
				echo "Warning: No failover time found"
			fi

			reset-all
		) &

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
screen -t node${i} ssh -t ${USER}@${host}.${DOMAIN} ${CMD}
logfile logs/log_${i}.txt
log on
EOF
		done

		screen -c "$tmp_screen"
		rm "$tmp_screen"

	done

else
	usage
fi
