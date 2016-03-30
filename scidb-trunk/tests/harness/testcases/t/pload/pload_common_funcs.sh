#!/bin/bash

DATA_GEN=${TEST_UTILS_DIR}/mp_data_gen.py # Data generator script.

# Cleanup function: removes the loaded array and data files.
run_cleanup()
{
    echo "Cleaning up on exit..."
    ${IQUERY_CMD} -aq "remove(dummy_01_A_${MY_PID})" > /dev/null 2>&1
    ${IQUERY_CMD} -aq "remove(real_01_A_${MY_PID})" > /dev/null 2>&1
    # Remove the generated files.
    rm /tmp/${MY_PID}_input* > /dev/null 2>&1
    for i in $(seq 0 $((${NUM_INSTANCES} - 1))) ;
    do
        ssh ${SERVER_IPS[i]} rm ${DATA_DIRS[i]}/${MY_PID}_input > /dev/null 2>&1
    done
    echo "Done with cleanup."
}

copy_file_to_host()
{
    local host=${1}
    local src_file_path=${2}
    local dst_file_path=${3}
    #local method=${4}

    if [ "${USE_PIPE}" == "1" ] ;
    then
        # Create a pipe in each instance's home directory.
	    ssh ${host} mkfifo -m 777 ${dst_file_path} > /dev/null 2>&1

        # Copy the file to the instance.
	    scp ${src_file_path} ${host}:${dst_file_path} > /dev/null 2>&1 &
    else
        # Copy the file to the instance.
	    scp ${src_file_path} ${host}:${dst_file_path} > /dev/null 2>&1
    fi
}
setup_array_info()
{
    local nulls=${1}
    types=(
	uint32 NULL DEFAULT 2603706653
	char   ""   ""      ""
	int64  ""   ""      ""
	bool   ""   ""      ""
	int8   NULL DEFAULT -34
	int32  NULL ""      ""
	uint64 ""   ""      ""
	float  NULL DEFAULT 0.496736187259
	char   NULL DEFAULT "char('d')"
	uint64 NULL DEFAULT 551329845711208649
	float  ""   ""      ""
	uint64 ""   ""      ""
	int8   NULL DEFAULT -6
	float  ""   DEFAULT 0.464320193393
	int16  ""   ""      ""
	uint64 NULL DEFAULT 1452353488656869443
	float  ""   DEFAULT 0.764646969759
	int8   ""   ""      ""
	uint32 NULL DEFAULT 2397589569
	bool   ""   ""      ""
	bool   NULL DEFAULT true
	int8   ""   DEFAULT 73
	char   NULL DEFAULT "char('x')"
	uint8  ""   DEFAULT 163
	string NULL DEFAULT "'z'"
	uint32 ""   DEFAULT 901045689
    )
    # Set up the full attributes string.
    ATTRS=""
    ATTR_BIN_TYPES=""
    local index=0
    for i in $(seq 0 4 $((${#types[@]} - 1))) ;
    do
	if [ "${ATTRS}" != "" ] ;
	then
	    ATTRS="${ATTRS},"
	    ATTR_BIN_TYPES="${ATTR_BIN_TYPES},"
	fi
	ATTRS="${ATTRS}a_${index}:${types[$i]}"
	ATTR_BIN_TYPES="${ATTR_BIN_TYPES}${types[$i]}"
	if [ "${nulls}" == "all" ] ;
	then
	    ATTRS="${ATTRS} NULL"
	    ATTR_BIN_TYPES="${ATTR_BIN_TYPES} NULL"
	else
	    if [ "${types[$(($i+1))]}" == "NULL" ] ;
	    then
		ATTRS="${ATTRS} NULL"
		ATTR_BIN_TYPES="${ATTR_BIN_TYPES} NULL"
	    fi
	fi
	if [ "${types[$(($i+2))]}" == "DEFAULT" ] ;
	then
	    ATTRS="${ATTRS} DEFAULT ${types[$(($i+3))]}"
	fi
	index=$((${index} + 1))
    done
}

MY_PID=${HPID}
IQUERY_CMD="iquery -c ${IQUERY_HOST:=127.0.0.1} -p ${IQUERY_PORT:=1239}"

# Seed the random number generator.
RANDOM=$((${RANDOM} % 4))

NUM_INSTANCES=$(${IQUERY_CMD} -ocsv:l -aq "list('instances')" | sed 1d | wc -l)

# Find IPs of all scidb instances.
SERVER_IPS=(
$(${IQUERY_CMD} -ocsv:l -aq "list('instances')" | sed 1d | sed s/,/\ /g | sed s/\'//g | awk '{print $1}')
)

# Find home/data directories of all scidb instances.
DATA_DIRS=(
$(${IQUERY_CMD} -ocsv:l -aq "list('instances')" | sed 1d | sed s/,/\ /g | sed s/\'//g | awk '{print $6}')
)

USE_PIPE=0 # Boolean flag for using pipes to copy data files to instances.

while [ $# != 0 ]
do
    case $1 in
	-p | --pipe) # Boolean flag: no arguments.
	    USE_PIPE=1
	    ;;
	-u | --upper-bound) # Upper bound for the flat array.
	    DIM_HI=$2 # Record the upper bound value.
	    shift
	    ;;
	-s | --size) # Size of one matrix dimension.
	    DIM_SZ=$2 # Record the size of the dimension.
	    shift
	    ;;
	-a | --adjust-size) # Number to add/subtract from the total data size.
	    ADJUST_SZ=$2
	    shift
	    ;;
	-i | --instance) # Which instance gets the "odd" chunk ("first","last", or "random").
	    ODD_CHUNK_INST=$2
	    shift
	    ;;
	-f | --format) # Data format.
	    DATA_FORMAT=$2
	    shift
	    ;;
	*)  echo 1>&2 "Unknown parameter ($1)!"
	    exit 1
    esac
    shift
done
