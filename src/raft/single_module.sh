#!/bin/bash

rm -rf single_module
mkdir single_module

if [ $# -eq 1 ] && [ "$1" = "--help" ]; then
	echo "Usage: $0 [RUNS=100] [PARALLELISM=#cpus] [TESTPATTERN='']"
	exit 1
fi

# If the tests don't even build, don't bother. Also, this gives us a static
# tester binary for higher performance and higher reproducability.
if ! go test -v -c -o tester; then
	echo -e "\e[1;31mERROR: Build failed\e[0m"
	exit 1
fi

# Default to 100 runs unless otherwise specified
runs=100
if [ $# -gt 0 ]; then
	runs="$1"
fi

# Default to one tester per CPU unless otherwise specified
parallelism=$(grep -c processor /proc/cpuinfo)
if [ $# -gt 1 ]; then
	parallelism="$2"
fi

# Default to no test filtering unless otherwise specified
test=""
if [ $# -gt 2 ]; then
	test="$3"
fi


logs=$(find . -maxdepth 1 -name 'test-*.log' -type f -printf '.' | wc -c)
success=$(grep -E '^PASS$' test-*.log | wc -l)
((failed = logs - success))

finish() {
	if ! wait "$1"; then
		if command -v notify-send >/dev/null 2>&1 &&((failed == 0)); then
			notify-send -i weather-storm "Tests started failing" \
				"$(pwd)\n$(grep FAIL: -- *.log | sed -e 's/.*FAIL: / - /' -e 's/ (.*)//' | sort -u)"
		fi
		((failed += 1))
	else
		((success += 1))
	fi

	if [ "$failed" -eq 0 ]; then
		printf "\e[1;32m";
	else
		printf "\e[1;31m";
	fi

	printf "Done %03d/%d; %d ok, %d failed\n\e[0m" \
		$((success+failed)) \
		"$runs" \
		"$success" \
		"$failed"
}

waits=() # which tester PIDs are we waiting on?
is=()    # and which iteration does each one correspond to?

# Cleanup is called when the process is killed.
# It kills any remaining tests and removes their output files before exiting.
cleanup() {
	for pid in "${waits[@]}"; do
		kill "$pid"
		wait "$pid"
		rm -rf "test-${is[0]}.err" "test-${is[0]}.log"
		is=("${is[@]:1}")
	done
	exit 0
}
trap cleanup SIGHUP SIGINT SIGTERM

# Run remaining iterations (we may already have run some)
for i in $(seq "$((success+failed+1))" "$runs"); do
	# If we have already spawned the max # of testers, wait for one to
	# finish. We'll wait for the oldest one beause it's easy.
	if [[ ${#waits[@]} -eq "$parallelism" ]]; then
		finish "${waits[0]}"
		waits=("${waits[@]:1}") # this funky syntax removes the first
		is=("${is[@]:1}")       # element from the array
	fi

	# Store this tester's iteration index
	# It's important that this happens before appending to waits(),
	# otherwise we could get an out-of-bounds in cleanup()
	is=("${is[@]}" $i)

	# Run the tester, passing -test.run if necessary
	if [[ -z "$test" ]]; then
		./tester -test.v 2> "./single_module/test-${i}.err" > "./single_module/test-${i}.log" &
		pid=$!
	else
		./tester -test.run "$test" -test.v 2> "./single_module/test-${i}.err" > "./single_module/test-${i}.log" &
		pid=$!
	fi

	# Remember the tester's PID so we can wait on it later
	waits=("${waits[@]}" $pid)
done

# Wait for remaining testers
for pid in "${waits[@]}"; do
	finish "$pid"
done

if ((failed>0)); then
	exit 1
fi
exit 0

# rm -rf res
# mkdir res

# for ((i = 0; i < 100; i++))
# do
#     # replace job name here
#     (go test -v -run TestSnapshotBasic2D -race) &> ./res/$i &

#     sleep 5

#     grep -nr "FAIL.*raft.*" res

# done
