#!/bin/bash
#
# Test script for parallel loading of binary data.  This
# script generates slightly larger or smaller amounts of
# data for loading into an array.  The variation in the
# size of the generated data is +-10% of the flat array's
# chunk size.

source ${TESTDIR}/pload_common_funcs.sh

trap "run_cleanup" 0 # Set up the cleanup callback.

# Set up some size info for the 2D array.
d0_size=${DIM_SZ}
d1_size=${d0_size}
array_size=$[ ( ${d0_size} * ${d1_size} )]
dummy_chunk=$[ ( ${array_size} / ${NUM_INSTANCES} ) ]

# Number of records to add/subtract from the total amount of records generated for the array.
percentages=(0.1 0.01 0.02 0.03 0.04 0.05 0.06 0.07 0.08 0.09)
perc_index=$[ ( ${RANDOM} % 10 ) ]
percent=${percentages[perc_index]}
ADJUST_ARRAY_SIZE_BY=$(echo "(${dummy_chunk} * ${percent})/1" | bc)
if [ $[ ( ${RANDOM} % 2 ) ] -eq "0" ] ;
then
    ADJUST_ARRAY_SIZE_BY="-${ADJUST_ARRAY_SIZE_BY}"
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

# Put together a list of file sizes the split binary data.
SPLIT_CHUNKS=""
if [ "${NUM_INSTANCES}" -gt "1" ] ;
then
    for i in $(seq 1 $((${NUM_INSTANCES} - 1))) ; # Iterate one less than the number of instances.
    do
        SPLIT_CHUNKS="${SPLIT_CHUNKS}${dummy_chunk}," # Add the same chunk size.
    done
    # Last chunk will be slightly smaller/bigger.
    LAST_CHUNK=$[ ( ${dummy_chunk} + ( ${ADJUST_ARRAY_SIZE_BY} ) ) ]
    SPLIT_CHUNKS=${SPLIT_CHUNKS}${LAST_CHUNK}
else
    SPLIT_CHUNKS=1
fi

echo "Creating array:"
${IQUERY_CMD} -aq "create array dummy_01_A_${MY_PID} ${FLAT_SCHEMA}"

# Generate binary output and split it into chunks (files).
${DATA_GEN} --dims-sizes ${d0_size},${d1_size} --seed 1234 --format binary --null-rate 0.08 -w 3 --adjust-count=${ADJUST_ARRAY_SIZE_BY} --split=${SPLIT_CHUNKS} --base-name /tmp/${MY_PID}_input "${SCHEMA}"

# To introduce some variation into the test, randomize the selection of the split data chunk for
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

echo "Count non-empty a_1 attributes in the array after redimension:"
n_a_1=$(${IQUERY_CMD} -ocsv:l -aq "aggregate(redimension(dummy_01_A_${MY_PID},${SCHEMA}),count(a_1))" | sed 1d)
expected_n_a_1=$[ ( ${array_size} + ( ${ADJUST_ARRAY_SIZE_BY} ) ) ]

if [ "${n_a_1}" -ne "${expected_n_a_1}" ] ;
then
    echo "FAIL: array size adjusted by: ${ADJUST_ARRAY_SIZE_BY}, count(a_1) = ${n_a_1}, expected = ${expected_n_a_1}!"
else
    echo "PASS: count(a_1) = expected!"
fi

echo "Done."
