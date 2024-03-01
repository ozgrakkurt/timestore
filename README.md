# timestore
Fully serializable on-disk datastore for time data. Built on glommio (direct IO and io_uring).

## Warning: this is mostly a sketch and it is designed for a very specific use case, it might not fit your use case.
## Warning: it is not tested, and it is intended to run on only latest linux kernels and nvme ssds

## Goals
It is a key-multivalue store so each key can have a set number of values.

Has absolutely no locking or synchronization(only uses atomic integers in some places) mechanism because it is written from
 scratch with ordered and append only workloads.

Iteration is super fast because everything is ordered and written to flat files.

All keys and offsets are kept in memory for reading so this will use a memory budget of `num_keys * (values_per_key + 1) * 8` bytes.

Fully crash resistant.
