<h1>KV Datastore</h1>
This repo is an implementation of Key Value based datastore similar to the modern key value databases.

The objective was to implement network-available persistent Key/Value system that
exposes the interfaces given below

1. Put(Key, Value)
2. Read(Key)
3. ReadKeyRange(StartKey, EndKey)
4. BatchPut(..keys, ..values)
5. Delete(key)

<h1> Steps to run the functionalities </h1>

1. Download the repo using git clone https://github.com/Vineethanand/KVDataStoreSystem.git.
2. cd to the path where the repo is downloaded.
3. Set the PYTHONPATH such that module imports work properly. Append the path to the repo in PYTHONPATH, ie \
export PYTHONPATH=$PYTHONPATH:<path_to_the_repo>.
4. Run the command python python3 kv_datastore_system/controllers/kv_datastore_controller.py --data-dir <path_to_the_datadir> --mem-threshold 1000.
The datadir path should be having write permissions, since this implementation uses filewrites for persistent data storage. The argument "--mem-threshold" is the maximum in memory (key,value) pair that is supported. By default 1000 is used, So that if more than 1000 keys are there in memory occupying RAM, the in memory data will be cleared and will be persisted to .json files in the disk called "disk_table" files and will have a prefix "disk_table" with a timestamp appended to it. While reading these .json files are also parsed to check whether the keys are present. Also every 10 minutes(currently hardcoded to 10 mins, but can be controlled from backend using a config value) a compaction method will merge these files to delete duplicate entries and deleted keys and write a merged file to the disk.

5. Now the server should be started on localhost:8080 which is the default port. It can be changed using --port argument while starting the server command. ie, \
python3 kv_datastore_system/controllers/kv_datastore_controller.py --data-dir <path_to_the_datadir> --mem-threshold 1000 --port 9000.

<h1> PUT interface </h1>

1. After starting the server. Run the below command to put a single key, value pair to the datastore.
2. <code> curl -X POST http://localhost:8080/put   -H "Content-Type: application/json"  -d '{"key": "a", "value" : "a_value"}' </code>
3. Status message in the client should show OK if the entry is successfull, otherwise an error message will appear.

<h1> READ interface </h1>

1. After starting the server. Run the below command to read the value corresponding to a key in the datastore.
2. <code> curl -X GET http://localhost:8080/read?key=a </code>
3. Status message should show the value of the key you have requested for.

<h1> DELETE interface </h1>

1. After starting the server. Run the below command to delete an entry in the datastore.
2. <code> curl -X DELETE http://localhost:8080/delete?key="a" </code>
3. Status message should show the "deleted".
4. To test whether the key is actually deleted you can run a READ query again and check the value.
5. If the key is not present in the datastore, the client receives a "null" value.

<h1> READKEYRANGE interface </h1>

1. After starting the server. Run the below command to read a range of values corresponding to keys between 'start' and 'end'.
2. <code> curl -G "http://localhost:8080/range" --data-urlencode "start=key1"  --data-urlencode "end=key2" </code>
3. Status message should show the list of values corresponding to the range if any exists in the range. Otherwise it will show "null".

<h1> BATCHPUT interfae </h1>

1. After starting the server. Run the below command to put a batch of key, value pairs to the datastore.
2. <code> curl -X POST http://localhost:8080/batch_put -H "Content-Type: application/json" -d '{"batch": {"a": "a_value", "aa": "aa_value", "b" : "b_value", "c" : "c_value"}}' </code>
3. Status message should "ok" if the write is successfull, otherwise it will show an error.
