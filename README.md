# arraybridge

## Usage

### scan_hdf5() operator

1. Create an array in SciDB:
~~~
create_array(array0, <val:double>[i=0:9999,1000,0]);
~~~

2. Create a configuration file with the path of the HDF5 file and the attribute/dataset mapping (optional). If the attribute/dataset mapping is not available, ArrayBridge looking for the dataset with the same name.
~~~
path_to_hdf5_file
attribute0 dataset0
attribute1 dataset1
~~~

3. Use scan_hdf5() operator to access the external array.
~~~
scan_hdf5(array0, path_to_config_file);
~~~

If the second parameter is omitted, ArrayBridge looks for the file $HOME/scidb_hdf5.config

### save() operator

Use 'hdf5' format to save as HDF5 dataset:

save(array0, 'test1.hdf5:dataset', -2, 'hdf5');
