source setup.conf

set -u

dynamic_dir="dynamic_${exp}"
src_dir="/users/${username}/${ds}"
exp_dir="${src_dir}/experiments"
stress_dir="${src_dir}/tools/stress"
output_dir_base="${exp_dir}/${dynamic_dir}"
exp_uid=$(date +%s)
output_dir="${output_dir_base}/${exp_uid}"
num_nodes_per_dc=$((nservers / ndcs))

# assign node numbers to servers and clients
s=0
for dc in $(seq 0 $((ndcs - 1))); do
	  for i in $(seq $((s+1)) $((num_nodes_per_dc+s))); do
    	  servers_by_dc_auto[$dc]+="${ipprefix}${i}${ippostfix} "
    	  clients_by_dc_auto[$dc]+="${ipprefix}$((i+nservers))${ippostfix} "
	  done
    s=$i
done

# csv ??, doesn't seem to be used when node prefix is "node-"
first_dc_servers_csv=$(echo ${servers_by_dc_auto[0]} | sed 's/ /,/g')

dcl_config=${nservers}_in_emulab
client_config=${nservers}_clients_in_emulab

dcl_config_full="${src_dir}/vicci_dcl_config/${dcl_config}"

# not entirely sure
strategy_properties="DC0:1"
for i in $(seq 1 $((ndcs - 1))); do
    strategy_properties=$(echo ${strategy_properties}",DC${i}:1")
done

# path of script that kills stress test
stress_killer="${src_dir}/kill_stress_kodiak.bash"

var=()
total_keys=()
for varANDkey in ${indep_values}; do
    var+=($(echo ${varANDkey} | awk -F":" '{ print $1 }'))
    total_keys+=($(echo ${varANDkey} | awk -F":" '{ print $2 }'))
done

init() {
    mkdir -p ${output_dir}
    rm -rf ${output_dir_base}/latest
    ln -s ${output_dir} ${output_dir_base}/latest
}

setup() {
    indep=${var[${i}]}
    keys=${total_keys[${i}]}
    keys_per_client_first=$((keys / num_nodes_per_dc))
    keys_per_client=$((keys / nservers))
    cli_output_dir="/local/${dynamic_dir}/${exp_uid}/${ds}/trial${t}"
    populate_attempts=0

    case $exp in
        size)
	          value_size=$indep
	          ;;
        columns_per_read)
	          cols_per_key_read=$indep
	          ;;
        columns_per_write)
	          cols_per_key_write=$indep
	          ;;
        keys_per_read)
	          keys_per_read=$indep
	          ;;
        keys_per_write)
	          keys_per_write=$indep
	          ;;
        write_frac)
	          write_frac=$indep
	          ;;
        write_trans_frac)
	          write_trans_frac=$indep
	          ;;
    esac

    data_file_name=${keys}_${value_size}_${cols_per_key_read}_${cols_per_key_write}_${keys_per_read}_${keys_per_write}_${write_frac}_${write_trans_frac}_${run_time}+${indep}+data
}

launch() {
    while [ 1 ]; do
	      ./kodiak_dc_launcher.bash
	      return_value=$?

	      if [ $return_value -eq 0 ]; then
	          break
	      fi
    done
}

set_server_keyspace() {
    for i in $(seq 3); do
	      (sleep 60; killall stress) & killall_jck_pid=$!
	      ${src_dir}/tools/stress/bin/stress --nodes=$first_dc_servers_csv --just-create-keyspace --replication-strategy=NetworkTopologyStrategy --strategy-properties=$strategy_properties
	      kill $killall_jck_pid
	      sleep 5
    done
}

set_client_keyspace() {
    set +x
    while [ 1 ]; do
	      (sleep $KILLALL_SSH_TIME; killall ssh) & killall_ssh_pid=$!
	      pop_pids=""
	      for (( dc = 0; dc < ${ndcs}; dc++ )) do
	          local_servers_csv=$(echo ${servers_by_dc_auto[$dc]} | sed 's/ /,/g')

	          for index in $(seq 0 $((num_nodes_per_dc - 1))); do
		            client=$(echo ${clients_by_dc_auto[$dc]} | sed 's/ /\n/g' | head -n $((index+1)) | tail -n 1)
		            # Write to All so the cluster is populated everywhere
		            ssh $client -t -t -o StrictHostKeyChecking=no "\
		     sudo mkdir -p /local/${dynamic_dir}/${exp_uid}; \
		     sudo chown ${username} /local/${dynamic_dir}; \
		     sudo chown ${username} /local/${dynamic_dir}/${exp_uid};"
		            ssh $client -o StrictHostKeyChecking=no "$stress_killer; \
		     cd ${src_dir}/tools/stress; bin/stress --nodes=$first_dc_servers_csv \
		     --columns=$cols_per_key_read --column-size=$value_size --operation=${insert_cmd} \
		     --consistency-level=LOCAL_QUORUM --replication-strategy=NetworkTopologyStrategy \
		     --strategy-properties=$strategy_properties --num-different-keys=$keys_per_client_first\
		     --num-keys=$keys_per_client_first --stress-index=$index \
		     --stress-count=$num_nodes_per_dc \
		     --file=${src_dir}/stress.out \
		      > >(tee /local/${dynamic_dir}/${exp_uid}/populate${t}.out) \
		     2> >(tee /local/${dynamic_dir}/${exp_uid}/populate${t}.err)" \
		                2>&1 | awk '{ print "'$client': "$0 }' & pop_pid=$!
		            pop_pids="$pop_pids $pop_pid"
		            echo "pop_pids: ${pop_pids}"
	          done
	      done

	          for pop_pid in $pop_pids; do
	              echo "Waiting on $pop_pid"
	              wait $pop_pid
	          done

	          kill -9 $killall_ssh_pid
	          killed_killall=$?
	          if [ $killed_killall == "0" ]; then
	              echo KILLEDKILLALL
	              break;
	          fi

	          ((populate_attempts++))
	          if [[ $populate_attempts -ge $MAX_ATTEMPTS ]]; then
	              echo -e "\n\n \e[01;31m Could not populate the cluster after $MAX_ATTEMPTS attempts \e[0m \n\n"
	              exit
	          fi

	          echo -e "\e[01;31m Failed populating $populate_attempts times, trying again (out of $MAX_ATTEMPTS) \e[0m"
        done
}

run_experiment() {
    for (( dc = 0; dc < ndcs; dc++ )) do
	      local_servers_csv=$(echo ${servers_by_dc_auto[$dc]} | sed 's/ /,/g')

	      for index in $(seq 0 $((num_nodes_per_dc - 1))); do
	          client=$(echo ${clients_by_dc_auto[$dc]} | sed 's/ /\n/g' | head -n $((index+1)) | tail -n 1)

	          ssh $client -t -t -o StrictHostKeyChecking=no "\
		sudo mkdir -p $cli_output_dir; sudo chown ${username} $cli_output_dir;"
	          ssh $client -o StrictHostKeyChecking=no "cd ${src_dir}/tools/stress; \
	    	((bin/stress --progress-interval=1 --nodes=$local_servers_csv --operation=DYNAMIC \
	    	--consistency-level=LOCAL_QUORUM --replication-strategy=NetworkTopologyStrategy \
	    	--strategy-properties=$strategy_properties --num-different-keys=$keys --stress-index=$index \
	    	--stress-count=$num_nodes_per_dc --num-keys=20000000 --column-size=$value_size \
	    	--columns-per-key-read=$cols_per_key_read --columns-per-key-write=$cols_per_key_write \
	    	--keys-per-read=$keys_per_read --keys-per-write=$keys_per_write --write-fraction=$write_frac \
	    	--write-transaction-fraction=$write_trans_frac --threads=$threads \
	    	 > >(tee ${cli_output_dir}/${data_file_name}) \
	    	2> ${cli_output_dir}/${data_file_name}.stderr) &); \
	    	sleep $((run_time+10)); ${src_dir}/kill_stress_kodiak.bash" \
	    	        2>&1 | awk '{ print "'$client': "$0 }' &
	      done
    done

        # Wait for all Clients to Finish
        wait
}

kill_all_ds() {
    set -m # need to monitor mode to fg processes
    for ip in ${servers_by_dc_auto[@]}; do
	      ssh -t -t -o StrictHostKeyChecking=no $ip "${ds}/kill_all_cassandra.bash"
    done
}

gather_results() {
    for (( dc = 0; dc < ndcs; dc++ )) do
	      for index in $(seq 0 $((num_nodes_per_dc-1))); do
	          client_dir=${output_dir}/client${index}
	          client=$(echo ${clients_by_dc_auto[$dc]} | sed 's/ /\n/g' | head -n $((index+1)) | tail -n 1)
	          rsync -az $client:/local/${dynamic_dir}/${exp_uid}/* $client_dir
	      done
    done
}
