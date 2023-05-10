# cpsc526-final
CPSC526 final project: BigTable prototype

## Requirement

- etcd

    We assume the `etcd` is installed. If not, you can install `etcd` by following this [guide](https://etcd.io/docs/v3.5/install/).

## Simple Setup

Several scripts are provided inside folder `scripts/`. To run the demo client, you can run the script `run_simple_client_test.sh` by 

``` bash
# cd to the scripts folder.
./run_simple_client_test.sh
```

> It will start etcd if necessary,start master and 1 server (in this simple setup) in sequence. Then it will start the client which will run a simple set and get test. After the running, the script will automatically run a clean up script that remove temporary files.

> If the test is run through the script mentioned above, both master and server log will be written to `server/master/master_run.log` and `server/server/server_run.log` respectively.


