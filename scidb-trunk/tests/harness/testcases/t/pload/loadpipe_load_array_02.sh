#!/bin/bash
# Test for loading data into SciDB using loadpipe.
# The test loads a large 3D array.
run_cleanup()
{
    echo "Cleaning up on exit..."
    iquery -c ${IQUERY_HOST:=127.0.0.1} -p ${IQUERY_PORT:=1239} \
	-aq "remove(real_02_A_0)" > /dev/null 2>&1
    echo "Done with cleanup."
}
trap "run_cleanup" 0
set -e
DATA_GEN=${TEST_UTILS_DIR}/mp_data_gen.py
LOADPIPE=${SCIDB_INSTALL_PATH}/bin/loadpipe.py

ATTRS="
a_36:int32 DEFAULT -2002752041,
a_37:int8 NULL DEFAULT 18,
a_38:int8 DEFAULT 117,
a_39:uint8 DEFAULT 7,
a_40:uint16 NULL,a_41:int32,
a_42:double,
a_43:double NULL,
a_44:uint64,
a_45:double NULL DEFAULT 0.281004243716,
a_46:uint8 NULL DEFAULT 186,
a_47:bool,
a_48:int32 NULL,
a_49:int8 DEFAULT 113,
a_50:float,
a_51:uint8 DEFAULT 99,
a_52:uint64 DEFAULT 1525016373539748765,
a_53:uint64,
a_54:bool DEFAULT false,
a_55:uint16 NULL DEFAULT 47660,
a_56:uint64 NULL DEFAULT 425369748435848363,
a_57:char,
a_58:string NULL,
a_59:float NULL,
a_60:int16 NULL DEFAULT -13655,
a_61:char DEFAULT char('e'),
a_62:bool NULL,
a_63:uint16,
a_64:int64 DEFAULT -5
"
SCHEMA="<
${ATTRS}
>
[
d_4=-10:*,29,1,
d_5=41:*,23,1,
d_6=-4:*,19,0
]"

FLAT_SCHEMA="<
d_4:int64,
d_5:int64,
d_6:int64,
${ATTRS}
>
[
dummy=0:*,1000000,0
]"

echo "Creating array:"
iquery -c ${IQUERY_HOST:=127.0.0.1} -p ${IQUERY_PORT:=1239} -aq "create array real_02_A_0 ${SCHEMA}"

echo "Loading data into array:"
${DATA_GEN} --dims-sizes 50,50,50 --seed 915087 --sparsity-rate 0.03 --null-rate 0.02 -n 3 "${SCHEMA}" \
 | ${LOADPIPE} -b 4MiB -s "${FLAT_SCHEMA}" -A real_02_A_0

echo "Ok."

iquery -ocsv:l -c ${IQUERY_HOST:=127.0.0.1} -p ${IQUERY_PORT:=1239} -aq "dimensions(real_02_A_0)"

echo "Done."