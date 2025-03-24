#!/usr/bin/env bash

# Run YDB CLI against the running server at `grpc://HOST:PORT`

DIR="/home/ubuntu/research-work-2025/ydbwork/ydb/"
YDB="./ydb/apps/ydb/ydb"
FILEPATH="/home/ubuntu/research-work-2025/ydbwork/test.csv"
HOST=localhost
PORT=50051

cd $DIR
$YDB --endpoint grpc://$HOST:$PORT --database /local import file csv --path /local/mytable "$FILEPATH"