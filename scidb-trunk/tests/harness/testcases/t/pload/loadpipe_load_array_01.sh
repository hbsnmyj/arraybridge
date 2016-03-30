#!/bin/bash
# Test for loading data into SciDB using loadpipe.
# The test loads a large 2D matrix.
run_cleanup()
{
    echo "Cleaning up on exit..."
    iquery -c ${IQUERY_HOST:=127.0.0.1} -p ${IQUERY_PORT:=1239} \
	-aq "remove(real_01_A_0)" > /dev/null 2>&1
    echo "Done with cleanup."
}
set -e

trap "run_cleanup" 0

DATA_GEN=${TEST_UTILS_DIR}/mp_data_gen.py
LOADPIPE=${SCIDB_INSTALL_PATH}/bin/loadpipe.py

ATTRS="
a_0:uint32 NULL DEFAULT 2603706653,
a_1:char,
a_2:int64,
a_3:bool,
a_4:int8 NULL DEFAULT -34,
a_5:int32 NULL,
a_6:uint64,
a_7:float NULL DEFAULT 0.496736187259,
a_8:char NULL DEFAULT char('d'),
a_9:uint64 NULL DEFAULT 551329845711208649,
a_10:float,
a_11:uint64,
a_12:int8 NULL DEFAULT -6,
a_13:float DEFAULT 0.464320193393,
a_14:int16,
a_15:uint64 NULL DEFAULT 1452353488656869443,
a_16:float DEFAULT 0.764646969759,
a_17:int8,
a_18:uint32 NULL DEFAULT 2397589569,
a_19:bool,
a_20:bool NULL DEFAULT true,
a_21:int8 DEFAULT 73,
a_22:char NULL DEFAULT char('x'),
a_23:uint8 DEFAULT 163,
a_24:string NULL DEFAULT 'z',
a_25:uint32 DEFAULT 901045689
"
SCHEMA="<
${ATTRS}
>
[
d_0=27:*,501,0,
d_1=-37:*,503,1
]"

FLAT_SCHEMA="<
d_0:int64,
d_1:int64,
${ATTRS}
>
[
dummy=0:*,1000000,0
]"

echo "Creating array:"
iquery -c ${IQUERY_HOST:=127.0.0.1} -p ${IQUERY_PORT:=1239} -aq "create array real_01_A_0 ${SCHEMA}"

echo "Loading data into array:"
PYTHONPATH=$PYTHONPATH:${SCIDB_INSTALL_PATH}/bin ${DATA_GEN} --dims-sizes 1000,1000 --seed 954335 --sparsity-rate 0.05 --null-rate 0.08 -w 3 "${SCHEMA}" \
 | ${LOADPIPE} -b 16MiB -s "${FLAT_SCHEMA}" -A real_01_A_0

echo "Ok."

iquery -ocsv:l -c ${IQUERY_HOST:=127.0.0.1} -p ${IQUERY_PORT:=1239} -aq "dimensions(real_01_A_0)"

echo "Done."