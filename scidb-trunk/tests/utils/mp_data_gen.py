#!/usr/bin/python
"""
Utility to generate random tsv/binary data based on an array schema.

@author: Igor Likhotkin
@date: February 16, 2015
"""
import sys
import os
import itertools as IT
import functools as FT
import re
import types
import random
import string
import time
import threading
import uuid
import argparse
import multiprocessing as MP
import Queue
import atexit
import struct
import threading
from scidblib import scidb_schema
from loadpipe import number

# Allowed characters for string attributes.
STRING_CHARS = [
    c for c in string.printable if c not in string.whitespace + '\'\"\\?'
    ]

# Maximum length of the string data.
MAX_RANDOM_STRING = 30

# Collection of functions for generating random values for
# different data types.
DATUM_GENERATORS = {
    'double': lambda : random.uniform(-1.0,1.0),
    'float': lambda : random.uniform(-1.0,1.0),
    'int64':lambda : random.randint(-2**63,2**63-1),
    'int32':lambda : random.randint(-2**31,2**31-1),
    'int16':lambda : random.randint(-2**15,2**15-1),
    'int8':lambda : random.randint(-2**7,2**7-1),
    'uint64':lambda : random.randint(0,2**64-1),
    'uint32':lambda : random.randint(0,2**32-1),
    'uint16':lambda : random.randint(0,2**16-1),
    'uint8':lambda : random.randint(0,2**8-1),
    'bool':lambda : random.choice([True,False]),
    'string': lambda: generate_random_string(),
    'char': lambda : random.choice(STRING_CHARS)
    }
# Collection of functions for generating random error values for
# different data types.
TSV_CSV_ERROR_GENERATORS = {
    'double': lambda : random.choice(['a','??','&','']),
    'float': lambda : random.choice(['x','%','!','']),
    'int64':lambda : random.choice(['y','$','-0.0','']),
    'int32':lambda : random.choice([str(2**31+1), '-' + str(2**31+1), '0.0', '']),
    'int16':lambda : random.choice([str(2**15+1), '-' + str(2**15+1), '0.0', '']),
    'int8':lambda : random.choice([str(2**7+1), '-' + str(2**7+1), '0.0', '']),
    'uint64':lambda : random.choice(['-1','*', '0.0', '']),
    'uint32':lambda : random.choice([str(2**32+1),'-1','0.1','']),
    'uint16':lambda : random.choice([str(2**32+1),'-1','0.1','']),
    'uint8':lambda : random.choice([str(2**8+1),'-1','0.1','']),
    'bool':lambda : random.choice(['()','9','+','']),
    'string': lambda: random.choice(['?000','']),
    'char': lambda : random.choice(['?0',''])
    }

# Map between scidb data types and Python binary struct pack format.
_struct_type_map = {
    'double':'d',
    'float': 'f',
    'int64': 'q',
    'int32': 'i',
    'int16': 'h',
    'int8':  'b',
    'uint64':'Q',
    'uint32':'I',
    'uint16':'H',
    'uint8': 'B',
    'bool':  '?',
    'string':'s',
    'datetime':'s',
    'datetimetz':'s',
    'char':  'b' # All characters are converted into bytes with ord function.
    }

# Map of TSV/CSV formats to their respective separator characters.
SEPARATOR_CHARS = {
    'tsv':'\t',
    'csv':',',
    'tsv:p':'|',
    'tsv:c':',',
    'csv:p':'|',
    'csv:t':'\t'
    }

# Default size of a dimension.
DEFAULT_DIM_SIZE = 10

# Default path for the base name of split output files (--base-name option).
DEFAULT_SLPIT_BASE_NAME = os.path.join(os.path.abspath(os.getcwd()), 'chunk')

_args = None # Command line arguments.

def binary_error(bin_record):
    i = random.choice(range(len(bin_record)))
    error_record = bin_record
    if (random.random() < 0.5):
        # Insert a byte.
        random_byte = struct.pack('B',random.randint(0,2**8-1))
        error_record = bin_record[:i] + random_byte + bin_record[i:]
    else:
        # Remove a byte.
        error_record = bin_record[:i] + bin_record[i+1:]
    return error_record

def tsv_csv_error(attrs,attr_types):
    # Pick out an attribute.
    i = random.choice(range(len(attrs)))
    attr_type = attr_types[i]
    attrs[i] = TSV_CSV_ERROR_GENERATORS[attr_type]()
    return attrs

def get_missing_reason_code():
    """ Generate missing reason code for a null data point.
    """
    return random.randint(1,2**7-1)

def format_tsv_csv(attr_types,record):
    """
    Format a single record into a TSV/CSV string.
    @param record list attribute values
    @return TSV/CSV representation of the data record
    """
    local_record = [] # Collection list for the formatted data.

    # Make sure there are no illegal characters in string attributes.
    assert all([
        re.search(r'[\\\t\r\n]+', attr) is None for attr in record if isinstance(attr,basestring)
        ])

    # Format all data points.
    for attr in record:
        if (attr is None): # This is a null.
            local_record.append('?'+str(get_missing_reason_code()))
            continue
        if (attr in [True,False]):
            attr = str(attr).lower()
        local_record.append(str(attr))

    if (random.random() < _args.error_rate):
        local_record = tsv_csv_error(local_record,attr_types)
    return SEPARATOR_CHARS[_args.format].join(local_record) + '\n'

def format_binary(attr_types,attr_nulls,record):
    """
    Convert the data record into a sequence of bytes.
    @param record list of attribute values for one array cell;
           None value indicates that the attribute is null (and
           nullable, of course)
    @return sequence of formatted binary data
    """
    packed_data = ''
    for attr,data_type,nullable in zip(record,attr_types,attr_nulls):
        if (data_type == 'char' and attr is not None):
            attr = ord(attr)
        if (nullable):
            null_byte = -1
            if (attr is None):
                null_byte = get_missing_reason_code()
            packed_data += struct.pack('=b',null_byte)
        if (data_type in ['string','datetime','datetimetz']):
            length = 0
            if (attr): # Value is not None.
                length = len(attr) + 1
                packed_data += struct.pack('=i',length)
                packed_data += attr
                packed_data += struct.pack('=b',0) # Null at the end of the string.
            else:
                packed_data += struct.pack('=i',0)
        else:
            if (attr is None):
                attr = 0
            packed_data += struct.pack('=' + _struct_type_map[data_type], attr)
    if (random.random() < _args.error_rate):
        packed_data = binary_error(packed_data)
    return packed_data

def emit_records(records_list,formatter_func,splitter=None):
    """ Output a list of array cell data.  If a splitter is provided,
    it will output the data in the appropriately split files.  Otherwise,
    all records will be formatted and printed to stdout.
    @param records_list list of array cell data which are lists of
           individual attribute values
    @param formatter_func function that formats array cell data for output;
           the function must take a single parameter - data record (a list of
           attribute values)
    @param splitter object that splits the formatted array cell data into
           separate output files
    """
    formatted_records = map( # Format the data first.
        formatter_func,
        records_list
        )
    if (splitter):
        splitter.emit(formatted_records)
    else:
        # Write the data to stdout.
        sys.stdout.write(''.join(formatted_records))

class ArraySplitter(object):
    """ Split the outgoing data into N files.
    """
    def __init__(
        self,
        chunk_sizes,
        base_name
        ):
        """ Constructor for the class.
        @param chunk_sizes list of chunk sizes to split the output into; if the
               output is larger than the sum of the specified chunks, the remainder will
               be written to the last file.
        @param base_name path prefix for the split files: base_name0, base_name1, etc.
        """
        self._chunk_sizes = chunk_sizes
        self._base_name = base_name

        self._files = ['{0}{1}'.format(self._base_name,i) for i in xrange(len(self._chunk_sizes))]

    def _write_records_to_file(
        self,
        records,
        file_name
        ):
        """ Write a list of records to the specified file.
        @param records list of formatted data records
        @param file_name name of the file where to write the records
        """
        with open(file_name,'ab') as fd:
            fd.write(''.join(records))

    def emit(self,records_list):
        """ Write out the list of formatted records.
        @param records_list list of formatted records
        """
        while (records_list): # Keeep writing as long as there are records in the list.
            if (self._chunk_sizes): # Check if there are still some chunks to write.
                # If the record list does not fit into one chunk, write out the whole
                # chunk, and save the remainfer for the next chunk.
                if (len(records_list) >= self._chunk_sizes[0]):
                    chunk = records_list[:self._chunk_sizes[0]]
                    records_list = records_list[self._chunk_sizes[0]:]
                    self._write_records_to_file(chunk,self._files[0])
                    self._chunk_sizes = self._chunk_sizes[1:]
                    # Advance to the next file if the current one is not last.
                    if (len(self._files) > 1):
                        self._files = self._files[1:]
                else: # The whole record list fits into current file: write it out.
                    self._chunk_sizes[0] = self._chunk_sizes[0] - len(records_list)
                    self._write_records_to_file(records_list,self._files[0])
                    records_list = []
            else:
                # No more chunks left to write, but there is still output.  Write it into
                # the last file.
                self._write_records_to_file(records_list,self._files[0])
                records_list = []

def generate_random_string():
    """
    Function that generates a randomly-selected string of characters
    for a string attribute.
    """
    # Make sure first char is not '?' to avoid being mistaken for an improper null value.
    s0 = random.choice(STRING_CHARS)
    s = ''
    if (MAX_RANDOM_STRING > 1):
        s = ''.join(random.sample(STRING_CHARS + ['?'],random.randint(1,MAX_RANDOM_STRING-1)))
    return s0 + s

def data_producer(
    queue,
    args_dict
    ):
    """
    Function that generates records (attributes for array
    cells) and deposits them into the queue for output.
    The function will be run as its own process on a CPU core.
    This means that arguments passed into this function must
    be of "plain" types (i.e. "pickle-able" objects).
    @param queue thread/process safe queue object where all of the
           records are inserted.
    @param args_dict dictionary object with all of the arguments:
           * dim_starts - starting indices for each dimension
           * dim_stops - stop indices for each dimension
           * attr_types - list of attribute data types
           * attr_nulls - list of booleans indicating if any of the
                          attributes are null-able
           * random_seed - numeric (integer) seed for the random
                           number generator
           * problem_size - total number of records this worker
                            should generate
    """
    start_indices = args_dict['dim_starts']
    stop_indices = args_dict['dim_stops']
    attr_types = args_dict['attr_types']
    attr_nulls = args_dict['attr_nulls']
    random_seed = args_dict['random_seed']
    problem_size = args_dict['problem_size']

    batch_size = number(_args.batch_size)
    sparsity_rate = _args.sparsity_rate
    null_rate = _args.null_rate
    error_rate = _args.error_rate

    random.seed(random_seed)

    dims_generator = make_dim_generator(
        start_indices,
        stop_indices
        )

    data_generator = make_record_generator(
        attr_types,
        attr_nulls,
        null_rate,
        error_rate
        )

    records_list = []

    # Generate all of the records for this worker in the loop.
    count = 0
    try:
        for record,coordinates in IT.izip(data_generator,dims_generator):
            if (sparsity_rate > 0.0) and (random.random() < sparsity_rate):
                count += 1
                continue

            if (len(records_list) == batch_size):
                queue.put(records_list)
                records_list = []
            if (_args.use_coordinates):
                record = list(coordinates) + record
            records_list.append(record)
            count += 1

            if (count == problem_size):
                break

        if (records_list): # Flush out any leftover records.
            queue.put(records_list)
    except (KeyboardInterrupt, SystemExit):
        # This is a child process: we need to catch CTRL-C or
        # unexpected exit here so that all of the workers (not
        # just the main process) quit.
        pass

def make_dim_generator(starts,stops):
    """ Generator for dimensions' coordinates.  First dimension
    is unbounded.  Each time the generator is "pumped" in
    a for loop or with next, it returns a tuple with coordinates
    dimension coordinates.
    @param starts list of starting indices for each dimension
    @param stops list of stopping indices for each dimension
    @return generator object
    """
    count = starts[0] # Starting index for the first dimension.
    while (True): # Generate first dimension forever.
        sequences = [ # Ranges for the other dimensions.
            xrange(*pair) for pair in zip(starts[1:],stops[1:])
            ]
        for c in IT.product(*sequences): # Cartesian product of the ranges.
            yield tuple([count] + list(c))
        count += 1 # Advance the index for the first dimension.

def make_record_generator(
    attr_types,
    attr_nulls,
    null_rate,
    error_rate
    ):
    """
    Generate a single data record (attribute values for one array cell).
    @param attr_types list of attribute types (strings).
    @param attr_nulls list of attribute nullable indicators.
    @param null_rate probability [0.0, 1.0) of a null for
           attribute values.
    @param error_rate probability [0.0, 1.0) of an error for
           attribute values.
    @return list of record attributes
    """

    while (True):
        record = []
        # Generate a randomly-selected value for each attribute type.
        for attr_type,nullable in zip(attr_types,attr_nulls):
            attr_generator = DATUM_GENERATORS[attr_type]
            # Check if a null needs to be generated
            if (nullable):
                if (null_rate > 0.0) and (random.random() < null_rate):
                    attr_generator = lambda : None
            record.append(attr_generator())
        # Return the full record.
        yield record

def _get_dim_sizes(dims):
    """ Calculate and return the sizes of all array dimensions.
    @param dims list of dimensions (Dimension superTuples).
    @return list of sizes (integers) of the specified dimensions
    """
    # If user did not specify dimension sizes, set them all equal to default.
    dims_sizes = [DEFAULT_DIM_SIZE] * len(dims)
    if (_args.dims_sizes):
        cmd_dims_sizes = _args.dims_sizes.split(',')
        if (len(cmd_dims_sizes) > len(dims)):
            msg = 'Too many dimension sizes specified: {0}, will use first {1}.\n'
            sys.stderr.write(msg.format(_args.dims_sizes,len(dims)))
        if (len(cmd_dims_sizes) < len(dims)):
            msg = 'Too few dimension sizes specified: {0}, will use defaults for last {1} dimension(s).\n'
            sys.stderr.write(msg.format(_args.dims_sizes,len(dims)-len(cmd_dims_sizes)))
        for i,size in enumerate(cmd_dims_sizes):
            if (i >= len(dims)):
                break
            dims_sizes[i] = int(size)
    return dims_sizes

def _get_dim_offsets(dims):
    """ Extract and return specified offsets for all array dimensions.
    @param dims list of dimensions (Dimension superTuples).
    @return list of offsets (integers) for the specified dimensions
    """
    dim_offsets = [0] * len(dims)
    if (_args.dim_offsets):
        cmd_offsets = _args.dim_offsets.split(',')
        if (len(cmd_offsets) > len(dims)):
            msg = 'Too many offsets specified: {0}, will use first {1}.\n'
            sys.stderr.write(msg.format(_args.dim_offsets,len(dims)))
        if (len(cmd_offsets) < len(dims)):
            msg = 'Too few offsets specified: {0}, will use 0 for last {1} dimension(s).\n'
            sys.stderr.write(msg.format(_args.dim_offsets,len(dims)-len(cmd_offests)))
        for i,offset in enumerate(cmd_offsets):
            if (i >= len(dims)):
                break
            dim_offsets[i] = int(offset)
    return dim_offsets

def _get_start_and_stop_indices(dims,dims_sizes,dim_offsets):
    """ Calculate and return lists of start and stop indices (for each dimension)
    for each worker.  Only the first dimension is divided among the workers.
    For instance, a 10x10x10 3D array will be divided among 3 workers as follows:
    - worker 1: 3x10x10
    - worker 2: 3x10x10
    - worker 3: 4x10x10
    The start and stop indices will reflect the problem division among the workers.
    @param dims list of array dimensions (Dimension superTuples)
    @param dims_sizes list of dimension sizes
    @param dim_offsets list of dimension index offsets
    @return two lists:
            - list of starting dimension indices for each worker
              (e.g. [[0,0],[2,2]] for a 2D array and 2 workers)
            - list of stopping dimension indices for each worker
              (e.g. [[2,2],[4,4]] for a 2D array and 2 workers)
    """
    # Number of workers that will be generating the data.
    n_workers = _args.workers

    # Set up the starting indices for each dimension for each worker.
    start_indices = []
    # Only the first dimension gets "sliced up" among the workers:
    # the other dimensions are left unchanged.
    # "Slice up" the first dimension among the workers.
    for i in xrange(n_workers):
        dim_index = dims[0].lo + (dims_sizes[0]/n_workers * i)
        dim_index += dim_offsets[0]
        worker_start_indices = [dim_index]
        worker_start_indices.extend([
            int(dim.lo + offset) for dim,offset in zip(dims[1:],dim_offsets[1:])
            ])
        start_indices.append(worker_start_indices)

    # Set up stop indices for each dimension for each worker.
    # Stopping indices in all dimensions (except the first one) are equal to
    # the dim_size + offset
    stop_indices = []
    # Set up all stop indices as dim_size + offset
    for i in xrange(len(start_indices)):
        stop_indices.append(
            [dims_sizes[i] + dim_offsets[i] + int(dim.lo) for i,dim in enumerate(dims)]
        )
    # Fix up the stop indices for the first dimension: first worker's stop index
    # for the first dimension is the same as the first dimension start index of
    # the second worker.  Last worker's stop indices remain as they were.
    for i,si in enumerate(stop_indices[:-1]):
        si[0] = start_indices[i+1][0]

    return start_indices,stop_indices

def _get_worker_problem_sizes(dims_sizes):
    """ Calculate and return the total number of array cells to generate for each
    worker.  The function enables specification of +N/-N cells to be generated by
    the last worker in the pool.
    @param dims_sizes list of dimension sizes
    @return list of cell numbers to generate for each worker
    """
    # Calculate the array size and sub-array sizes for each worker.
    n_workers = _args.workers
    final_record_adjustment = _args.adjust_count
    total_array_size = reduce(lambda x,y: x * y,dims_sizes,1) + (final_record_adjustment)
    prob_sizes = [dims_sizes[0]/n_workers * reduce(lambda x,y: x * y,dims_sizes[1:],1)] * n_workers
    prob_sizes[-1] = total_array_size - sum(prob_sizes[:-1])

    return total_array_size,prob_sizes

def _get_formatter(dims,attr_types,attr_nulls):
    """ Get the object to format the array cells for output.
    @param attr_types list of array attribute types
    @param attr_nulls list of booleans indicating if an attribute is nullable or not
    @param output formatter object
    """
    format_attr_types = attr_types
    format_attr_nulls = attr_nulls
    # Check if the coordinates are supposed to be added as attributes.
    if (_args.use_coordinates):
        format_attr_types = ['int64'] * len(dims)
        format_attr_types.extend(attr_types)
        format_attr_nulls = [False]  * len(dims)
        format_attr_nulls.extend(attr_nulls)

    # Pick the appropriate formatter based on user's specification.
    if (_args.format == 'binary'):
        formatter_func = FT.partial(format_binary,format_attr_types,format_attr_nulls)
    elif (_args.format in ('tsv','tsv:p','tsv:c','csv','csv:p','csv:t')):
        formatter_func = FT.partial(format_tsv_csv,format_attr_types)
    else:
        raise ValueError('Unsupported format: ' + _args.format)

    return formatter_func

def _get_splitter(total_array_size):
    """ Get output splitter object.  The function returns output splitter object
    only if user requested to split output into files.  Otherwise, the function returns
    None.
    @param total_array_size size of the user-specified array (sum of all dimension
           sizes plus the adjustment term (e.g. +1, -1, etc.)
    @return object for splitting the output into separate files (or a None value)
    """
    # Set up the emitter for the output.
    splitter = None
    if (_args.split):
        if (_args.base_name == DEFAULT_SLPIT_BASE_NAME):
            msg = 'Using default base name for the split output files: {0}.\n'
            sys.stderr.write(msg.format(_args.base_name))
        # Check if user specified the sizes (in terms of array cells) for each split file.
        chunks = [int(c) for c in _args.split.split(',')]
        split_chunks = chunks

        if (len(chunks) == 1): # User specified how many files to split output into.
            n_chunks = chunks[0]
            split_chunks = [total_array_size/n_chunks for i in xrange(n_chunks)]
            split_chunks[-1] = total_array_size - sum(split_chunks[:-1])

        splitter = ArraySplitter(split_chunks,_args.base_name)
    return splitter

def setup_constant_data_generators():
    """ Reset global datum generators to return contant values.
    """
    global DATUM_GENERATORS
    if (_args.constant == 0):
        DATUM_GENERATORS['double'] = lambda : 0.0
        DATUM_GENERATORS['float'] = lambda : 0.0
        for t in ['int64','int32','int16','int8','uint64','uint32','uint16','uint8']:
            DATUM_GENERATORS[t] = lambda: 0
        DATUM_GENERATORS['bool'] = lambda: False
        DATUM_GENERATORS['string'] = lambda: ''
        DATUM_GENERATORS['char'] = lambda : '\x00'
    else:
        DATUM_GENERATORS['double'] = lambda : float(_args.constant)
        DATUM_GENERATORS['float'] = lambda : float(_args.constant)
        for t in ['int64','int32','int16','int8','uint64','uint32','uint16','uint8']:
            DATUM_GENERATORS[t] = lambda: _args.constant
        DATUM_GENERATORS['bool'] = lambda: True
        DATUM_GENERATORS['string'] = lambda: str(_args.constant)
        DATUM_GENERATORS['char'] = lambda : struct.pack('b',_args.constant)

def remove_separators_from_strings():
    global STRING_CHARS
    if (_args.format != 'binary'):
        if (SEPARATOR_CHARS[_args.format] in STRING_CHARS):
            STRING_CHARS.remove(SEPARATOR_CHARS[_args.format])

def parse_args(argv):
    """
    Parse out the command line parameters.
    @return the namespace with all of the command line arguments.
    """
    global _args

    prog_desc = 'Data generator for SciDB arrays.'
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '-s',
        '--seed',
        type=int,
        default=random.randint(0,1000000),
        metavar='SEED',
        help='Seed for the random generator.'
        )
    parser.add_argument(
        '-e',
        '--error-rate',
        default=0.0,
        type=float,
        metavar='ERROR_RATE',
        help='Probability [0.0,1.0) of errors in the generated data.'
        )
    parser.add_argument(
        '--sparsity-rate',
        default=0.0,
        type=float,
        metavar='SPARSE_RATE',
        help='Probability [0.0,1.0) of a record skip (sparsity of data).'
        )
    parser.add_argument(
        '-n',
        '--null-rate',
        type=float,
        default=0.0,
        metavar='NULL_RATE',
        help='Probability [0.0,1.0) of null values for nullable attributes.'
        )
    parser.add_argument(
        'schema',
        metavar='SCHEMA',
        help='String representing the array schema for which to generate data.'
        )
    parser.add_argument(
        '--dims-sizes',
        default='',
        metavar='DIMS_SIZES',
        help='Comma-separated list of max number of cells for each dimension in the array schema (default is 4).'
        )
    parser.add_argument(
        '--format',
        default='tsv',
        choices=['tsv','tsv:p','tsv:c','csv','csv:p','csv:t','binary'],
        metavar='FORMAT',
        help='Output format (default is tsv).'
        )
    parser.add_argument(
        '--dim-offsets',
        default='',
        metavar='DIM_OFFSETS',
        help='Comma-separated list of index offsets for each dimension (default is zeros).'
        )
    parser.add_argument(
        '--batch-size',
        default='16K',
        metavar='BATCH_SIZE',
        help="""Number or records each worker generates before placing them into the output queue.
            Size multiplier suffixes can be used (e.g. 16K, 1MiB, 1G, etc.)  Default is 16K."""
        )
    parser.add_argument(
        '--no-coordinates',
        default=True,
        dest='use_coordinates',
        action='store_false',
        help='Flag to omit the dimensions coordinates from the data stream.'
        )
    parser.add_argument(
        '--adjust-count',
        metavar='COUNT',
        type=int,
        default=0,
        help='Number to add/subtract from the final record count (e.g. +1 or -1).'
        )
    parser.add_argument(
        '--split',
        metavar='CHUNKS',
        default=[],
        help="""Number or chunks to split output into or a comma-separated list of
        individual chunk sizes.  Use --base-name option to specify the path of the
        output files."""
        )
    parser.add_argument(
        '--base-name',
        metavar='BASENAME',
        default=DEFAULT_SLPIT_BASE_NAME,
        help=""""Base" file path to use when splitting the output into chunks.
        The splitter will create N files (N = number of chunks) with the name base_nameX
        where base_nameX where X is in range 0..(#chunks-1).  Default base name is ./chunk."""
        )
    parser.add_argument(
        '-w',
        '--workers',
        default=1,
        type=int,
        metavar='WORKERS',
        help='Number of workers that will be generating the output.'
        )
    parser.add_argument(
        '-c',
        '--constant',
        type=int,
        choices=range(-5,6),
        metavar='CONSTANT',
        help="""Integer value for all numeric data.  Allowed values are in range [-5,5].
        Strings will be set to string representation of the integer value with one
        exception: value of 0 will cause all strings to be 0-length.  Boolean attributes
        will be set to false if the specified value is 0 (true for all other values)."""
        )
    _args = parser.parse_args(args=argv[1:])

def main(argv=None):
    """
    Main program entry point.
    """

    if (argv is None):
        argv = sys.argv

    parse_args(argv) # Parse the command line arguments.

    # Filter out specific TSV/CSV separator character (if one of such
    # formats is specified).
    remove_separators_from_strings()

    # TODO: Fix for default_nullable=True, see SDB-5138.
    if 0:
        attrs,dims = scidb_schema.parse(_args.schema)
    else:
        attrs,dims = scidb_schema.parse(_args.schema, default_nullable=False)

    if (_args.constant is not None):
        setup_constant_data_generators()

    dims_sizes = _get_dim_sizes(dims)

    dim_offsets = _get_dim_offsets(dims)

    start_indices,stop_indices = _get_start_and_stop_indices(dims,dims_sizes,dim_offsets)

    total_array_size,prob_sizes = _get_worker_problem_sizes(dims_sizes)

    attr_types = [a.type for a in attrs]
    attr_nulls = [a.nullable for a in attrs]

    formatter_func = _get_formatter(dims,attr_types,attr_nulls)

    splitter = _get_splitter(total_array_size)

    # Set up the inter-process data manager.
    manager = MP.Manager()
    # Prepare the data queue for the data-generating workers.
    queue = manager.Queue()
    # Put together the list of arguments for all workers.
    args_dicts = []
    for i in xrange(_args.workers):
        arg_tuples = [
            ('dim_starts',start_indices[i]),
            ('dim_stops',stop_indices[i]),
            ('attr_types',attr_types),
            ('attr_nulls',attr_nulls),
            ('random_seed',_args.seed+i),
            ('problem_size',prob_sizes[i])
            ]
        args_dicts.append(dict(arg_tuples))

    # Create the process pool of workers.
    pool = MP.Pool(processes=_args.workers)

    # Assign data generating tasks to each worker in the pool and
    # start them.
    results = [pool.apply_async(data_producer,(queue,d),{}) for d in args_dicts]

    # Register a simple cleanup function in case of the unexpected exit
    # (e.g. user presses CTRL-C).
    atexit.register(lambda : pool.terminate())

    # Record the original parent process id: when the parent dies, we shall
    # attempt to exit too.
    ppid = os.getppid()

    # Process the data from the record queue: workers insert data record
    # blocks into the queue while the main (this) process pulls them out
    # and outputs them to stdout.

    queue_empty = False
    while (not all([r.ready() for r in results])) or (not queue_empty):
        try:
            s = queue.get(False) # Grab a block of text from the queue.
            queue_empty = False
            emit_records(s,formatter_func,splitter)
        except Queue.Empty:
            queue_empty = True
        except (KeyboardInterrupt, SystemExit):
            # In case of CTRL-C press or unexpected exit, exit the main
            # process of the program.  Child processes will also exit
            # in the same way.
            break

    # Close the pool (no more workers can be added).
    pool.close()
    # Double-check that all workers are finished.
    pool.join()
    # Terminate the pool.
    pool.terminate()

    return 0

# Program entry point: run main function.
if __name__ == '__main__':
    sys.exit(main())

