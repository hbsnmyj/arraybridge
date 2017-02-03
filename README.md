# ArrayBridge

## Installation

* Install the HDF5 library
    * On Ubuntu, hdf5 library can be installed via apt-get:
        sudo apt-get install libhdf5-dev
    * On other systems, HDF5 can be compiled manually (See https://support.hdfgroup.org/HDF5/release/obtain5110.html):
    ~~~~
        ./configure --enable-hl --enable-build-mode=production
        make
        sudo make install
    ~~~~
    You can also install the HDF5 library to non-default location:

    ~~~~
    ./configure --prefix=$PREFIX --enable-hl --enable-build-mode=production
    make -j12
    make PREFIX="$PREFIX" install
    ~~~~
    In that case, make sure HDF5 can be found by CMake:
    ~~~~
    export CMAKE_PREFIX_PATH=$PREFIX:$CMAKE_PREFIX_PATH
    ~~~~
    
* Following [SciDB's installation guide](https://paradigm4.atlassian.net/wiki/display/ESD/SciDB+Community+Edition+Installation+Guide) to install SciDB.

## Usage

### scan_hdf5() operator

* Create an array in SciDB:
~~~
create_array(array0, <val:double>[i=0:9999,1000,0]);
~~~~

* Create a configuration file with the path of the HDF5 file and the attribute/dataset mapping (optional). If the attribute/dataset mapping is not available, ArrayBridge looking for the dataset with the same name.
~~~
path_to_hdf5_file
attribute0 dataset0
attribute1 dataset1
~~~

* Use scan_hdf5() operator to access the external array.
~~~
scan_hdf5(array0, path_to_config_file);
~~~

If the second parameter is omitted, ArrayBridge looks for the file $HOME/scidb_hdf5.config

### save() operator

Use 'hdf5' format to save as HDF5 dataset:

save(array0, 'test1.hdf5:dataset', -2, 'hdf5');
