# timestore
Fully serializable on-disk datastore for time data.

## Goals
It is key-multivalue store so each key can have a set number of values.

Has absolutely no locking or synchronization mechanism because it is written from
 scratch with ordered and append only workloads.

Iteration is super fast because everything is ordered and written to flat files.

All keys and offsets are kept in memory for reading so this will use a memory budget of `num_keys * (values_per_key + 1) * 8` bytes.

Fully crash resistant.
