#!/bin/bash
#
# Test script for parallel loading of binary data.  This
# script attempts to load data files in parallel where one
# of the instances is missing its data file.
#

source ${TESTDIR}/pload_common_funcs.sh

trap "run_cleanup" 0 # Set up the cleanup callback.

# Set up some size info for the 2D array.
d0_size=${DIM_SZ}
d1_size=${d0_size}
array_size=$[ ( ${d0_size} * ${d1_size} )]
dummy_chunk=$[ ( ${array_size} / ${NUM_INSTANCES} ) ]

# Setup attributes and binary format strings (ATTRS and ATTR_BIN_TYPES).
setup_array_info set_nulls

# Schema of the array where to load the data.
SCHEMA="<
${ATTRS}
>
[
d_0=0:*,501,0,
d_1=0:*,503,1
]"

# Schema for the "flat" array where the dimensions appear as attributes.
FLAT_SCHEMA="<
d_0:int64,
d_1:int64,
${ATTRS}
>
[
dummy=0:${DIM_HI},${dummy_chunk},0
]"

echo "Creating array:"
${IQUERY_CMD} -aq "create array dummy_01_A_${MY_PID} ${FLAT_SCHEMA}"

# Generate binary data and split it into chunks (files).
${DATA_GEN} --dims-sizes ${d0_size},${d1_size} --seed 1234 --format binary --null-rate 0.5 -w 3 --split=${NUM_INSTANCES} --base-name /tmp/${MY_PID}_input "${SCHEMA}"

# Upload split up data files to each instance..
SKIP_INDEX=$[ ( ${RANDOM} % ${NUM_INSTANCES} ) ]
for i in $(seq 0 $((${NUM_INSTANCES} - 1))) ;
do
    if [ "${i}" -eq "${SKIP_INDEX}" ] ;
    then
	    continue
    fi
    # Copy the file to the instance.
    copy_file_to_host ${SERVER_IPS[i]} /tmp/${MY_PID}_input${i} ${DATA_DIRS[i]}/${MY_PID}_input
done

if [ "${NUM_INSTANCES}" -gt "1" ] ;
then
    err_msg="not found on instance"
else
    err_msg="Error id: scidb::SCIDB_SE_INFER_SCHEMA::SCIDB_LE_FILE_NOT_FOUND"
fi

echo "Loading data into array:"
if [ "${DATA_FORMAT}" == "binary" ] ;
then
    error_text=$(${IQUERY_CMD} -ocsv:l -naq "load(dummy_01_A_${MY_PID},'${MY_PID}_input',-1,'(int64,int64,${ATTR_BIN_TYPES})')" 2>&1 | grep "${err_msg}")
else
    error_text=$(${IQUERY_CMD} -ocsv:l -naq "load(dummy_01_A_${MY_PID},'${MY_PID}_input',-1,'${DATA_FORMAT}" 2>&1 | grep "${err_msg}")
fi

if [ "${error_text}" == "" ] ;
then
    echo "FAIL: no errors detected!"
else
    echo "PASS: correct error detected!"
fi

echo "Done."
