#!/bin/bash
# Test for loading data into SciDB using loadpipe.
# The test loads a large 3D array.
run_cleanup()
{
    echo "Cleaning up on exit..."
    iquery -c ${IQUERY_HOST:=127.0.0.1} -p ${IQUERY_PORT:=1239} \
	-aq "remove(real_04_A_0)" > /dev/null 2>&1
    echo "Done with cleanup."
}
set -e

trap "run_cleanup" 0

DATA_GEN=${TEST_UTILS_DIR}/mp_data_gen.py
LOADPIPE=${SCIDB_INSTALL_PATH}/bin/loadpipe.py

ATTRS="
a_289:int32 DEFAULT -1260098061,
a_290:char,
a_291:bool NULL DEFAULT false,
a_292:int8 DEFAULT 94,
a_293:int64 NULL DEFAULT -1,
a_294:int32,
a_295:uint8 NULL,
a_296:uint32 NULL DEFAULT 3950172571,
a_297:float NULL DEFAULT 0.872431640732,
a_298:int64 NULL DEFAULT 1,
a_299:int16 NULL DEFAULT -15022,
a_300:float,
a_301:float DEFAULT 0.0625623024562,
a_302:string NULL DEFAULT 'i',
a_303:int32 NULL DEFAULT 1530861568,
a_304:int32 NULL,
a_305:bool NULL DEFAULT false,
a_306:uint16 DEFAULT 23449,
a_307:bool NULL,
a_308:int16 DEFAULT -25616,
a_309:uint32 NULL DEFAULT 3682199341,
a_310:double NULL DEFAULT 0.926087313181,
a_311:uint64 DEFAULT 977210311288494025,
a_312:int16 DEFAULT 20967,
a_313:uint64 NULL,
a_314:int64 NULL DEFAULT -5,
a_315:uint64,
a_316:double DEFAULT 0.707907830863
"
SCHEMA="<
${ATTRS}
>
[
d_38=24:*,23,1,
d_39=6:*,19,0,
d_40=22:*,29,1
]"

FLAT_SCHEMA="<
d_38:int64,
d_39:int64,
d_40:int64,
${ATTRS}
>
[
dummy=0:*,1000000,0
]"

echo "Creating array:"
iquery -c ${IQUERY_HOST:=127.0.0.1} -p ${IQUERY_PORT:=1239} -aq "create array real_04_A_0 ${SCHEMA}"

echo "Loading data into array:"
${DATA_GEN} --dims-sizes 50,50,50 --seed 360590 --sparsity-rate 0.05 --null-rate 0.14 -n 3 "${SCHEMA}"\
| ${LOADPIPE} -b 2MiB -s "${FLAT_SCHEMA}" -A real_04_A_0

echo "Ok."

iquery -ocsv:l -c ${IQUERY_HOST:=127.0.0.1} -p ${IQUERY_PORT:=1239} -aq "dimensions(real_04_A_0)"
echo "Done."