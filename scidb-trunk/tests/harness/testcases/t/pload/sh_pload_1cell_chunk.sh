#!/bin/bash
#
# Test script for parallel loading of binary data.  This
# script creates uneven files for SciDB
# instances: one instance loads a file with one row of
# attributes while another one - a file with 2N-1 rows
# (N=flat array chunk size).
#

source ${TESTDIR}/pload_common_funcs.sh

trap "run_cleanup" 0 # Set up the cleanup callback.

# Set up some size info for the 2D array.
d0_size=${DIM_SZ}
d1_size=${d0_size}
array_size=$[ ( ${d0_size} * ${d1_size} )]
dummy_chunk=$[ ( ${array_size} / ${NUM_INSTANCES} ) ]

# Last row index (could be different based on how many more or less cells are generated).
LAST_ROW_INDEX=$[ ( ${array_size}  / ${d0_size} ) ]
index_remainder=$[ ( ${array_size} % ${d0_size} ) ]
if [ "${index_remainder}" -eq "0" ] ;
then
    LAST_ROW_INDEX=$[ ( ${LAST_ROW_INDEX} - 1 ) ]
fi

# Set up attributes and binary format strings (ATTRS and ATTR_BIN_TYPES).
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

# Split the chunks so that one chunk is twice the size of the dummy array size and
# one chunk is only of length 1 (other chunks are sized to match the dummy array
# chunk size.
SPLIT_CHUNKS=""
if [ "${NUM_INSTANCES}" -gt "1" ] ;
then
    # Set aside 2 chunks: one is "oversized" (2N-1, N = data size/NUM_INSTANCES)
    # and the second one is small - 1 cell only.
    BIG_CHUNK=$[ ( ( ${dummy_chunk} * 2 ) - 1 ) ]
    SPLIT_CHUNKS="${BIG_CHUNK},1"

    # Use dummy_chunk for the remaining instances' data files.
    for i in $(seq 2 $((${NUM_INSTANCES} - 1))) ;
    do
        SPLIT_CHUNKS="${SPLIT_CHUNKS},${dummy_chunk}"
    done
else
    # Everything will go into the same file in case of single-instance cluster.
    SPLIT_CHUNKS="${array_size},0"
fi

echo "Creating array:"
${IQUERY_CMD} -aq "create array dummy_01_A_${MY_PID} ${FLAT_SCHEMA}"

# Generate binary output and split it into even chunks (files).
${DATA_GEN} --dims-sizes ${d0_size},${d1_size} --seed 1234 --format ${DATA_FORMAT} --null-rate 0.08 -w 3 --split=${SPLIT_CHUNKS} --base-name /tmp/${MY_PID}_input "${SCHEMA}"

# To introduce some variation into the test, randomize the selection of the data file for
# each SciDB instance.
RANDOM_INDEX_START=$RANDOM # Generate a random number.

# Upload split up data files to each instance.
for i in $(seq 0 $((${NUM_INSTANCES} - 1))) ;
do
    # Pick a file to upload to an instance.
    file_index=$[ ( ( ${RANDOM_INDEX_START} + i ) % ${NUM_INSTANCES} ) ]

    # Copy the file to the instance.
    copy_file_to_host ${SERVER_IPS[i]} /tmp/${MY_PID}_input${file_index} ${DATA_DIRS[i]}/${MY_PID}_input
done

echo "Loading data into array:"
if [ "${DATA_FORMAT}" == "binary" ] ;
then
    ${IQUERY_CMD} -ocsv:l -naq "load(dummy_01_A_${MY_PID},'${MY_PID}_input',-1,'(int64,int64,${ATTR_BIN_TYPES})')"
else
    ${IQUERY_CMD} -ocsv:l -naq "load(dummy_01_A_${MY_PID},'${MY_PID}_input',-1,'${DATA_FORMAT}')"
fi

echo "Count non-empty d_0 attributes in loaded array:"
${IQUERY_CMD} -ocsv:l -aq "aggregate(dummy_01_A_${MY_PID},count(d_0))"

echo "Count non-empty d_1 attributes in loaded array:"
${IQUERY_CMD} -ocsv:l -aq "aggregate(dummy_01_A_${MY_PID},count(d_1))"

${IQUERY_CMD} -aq "create temp array real_01_A_${MY_PID} ${SCHEMA}"
${IQUERY_CMD} -ocsv:l -naq "store(redimension(dummy_01_A_${MY_PID},${SCHEMA}),real_01_A_${MY_PID})"

echo "Count non-empty a_1 attributes in first row of array after redimension:"
${IQUERY_CMD} -ocsv:l -aq "aggregate(filter(real_01_A_${MY_PID},d_0=0 and d_1<${d1_size}),count(a_1))"

echo "Count non-empty a_1 attributes in last row of array after redimension:"
${IQUERY_CMD} -ocsv:l -aq "aggregate(filter(real_01_A_${MY_PID},d_0=${LAST_ROW_INDEX} and d_1<${d1_size}),count(a_1))"

echo "Done."
