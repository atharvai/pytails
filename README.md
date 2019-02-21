# pyTails

pyTails is a script that allows reading MongoDB oplog and publish it.

Current Support:

Source
- :heavy_check_mark: oplog
- :x: Change Stream

Destination:
- :heavy_check_mark: console/stdout
- :heavy_check_mark: Kinesis Data Stream
- :x: Kinesis Firehose

## Configuration


| cmd arg | env var | description |
| ----- | ----- | ----- |
| `--mongo-host` | `MONGO_HOST` | MongoDB host|
| `--mongo-port` | `MONGO_PORT` | MongoDB port|
| `--replica-set` | `REPLICA_SET` | MongoDB Replica set name|
| `--tail-id` | `TAIL_ID` | Friendly unique identifier for cluster replica set. should be globally unique|
| `--kinesis-data-sink` | `KINESIS_DATA_SINK` | If specified should be Kinesis Data Stream name. (not arn). |
| `--firehose-data-sink` | `FIREHOSE_DATA_SINK` | If specified should be Firehose Delivery Stream name. (not arn). |
| `--console-sink` | `CONSOLE_SINK` | Flag. If specified prints records to console/stdout. `0` or `1` |
| `--debug` | `DEBUG` | Sets logging level to DEBUG. `0` or `1` |

## Output
All output data has the following fields:

- `doc`: oplog or full doc
- `ts`: ISO timestamp if specified

## Docker

Build:
```bash
docker build -t pytails:0.1.0 -f docker/pytails.Dockerfile .
```

Run:
```bash
docker run -it --e MONGO_HOST=xxx.xxx.xxx.xxx \
    -e MONGO_PORT=27017 \
    -e KINESIS_DATA_SINK=atharva_pytails \
    -e TAIL_ID=my-cluster \
    -e REPLICA_SET=rs1 \
    -v C:\Users\atharva.inamdar\.aws\:/root/.aws/:ro \
    pytails:0.1.0
```
