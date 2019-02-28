#!/usr/bin/python
# !-*- coding:utf8 -*-

'''
read flume metrics from flume JSON Reporting
flume-env.sh should add JAVA_OPTS: -Dflume.monitoring.type=http -Dflume.monitoring.port=3000
'''

import requests
import json
import time
import socket

hostname = socket.gethostname()
ts = int(time.time())
step = 60
GAUGE = "GAUGE"
COUNTER = "COUNTER"


def load(hostname, metric, timestamp, step, value, counterType, tags):
    msg = {
        "endpoint": hostname,
        "metric": metric,
        "timestamp": timestamp,
        "step": step,
        "value": value,
        "counterType": counterType,
        "tags": tags,
    }

    return msg


try:
    # this is your flume metrics http url, you should change is by your flume environment
    url = "http://127.0.0.1:8300/metrics"
    tags = "project=flume"
    v = requests.get(url)
    res = json.loads(v.text)
    payload = []
    for key in res:
        type = res[key]["Type"]
        if type == "SOURCE":
            for param in ["KafkaCommitTimer", "KafkaEventGetTimer", "OpenConnectionCount"]:
                metric = key.replace("SOURCE.", "flume.source.") + "." + param
                value = float(res[key][param])
                msg = load(hostname, metric, ts, step, value, GAUGE, tags)
                payload.append(msg)

            for param in ["AppendAcceptedCount", "AppendBatchAcceptedCount", "AppendBatchReceivedCount",
                          "AppendReceivedCount",  "ChannelWriteFail", "EventAcceptedCount", "EventReadFail",
                          "EventReceivedCount", "GenericProcessingFail", "KafkaEmptyCount"]:
                metric = key.replace("SOURCE.", "flume.source.") + "." + param
                value = float(res[key][param])
                msg = load(hostname, metric, ts, step, value, COUNTER, tags)
                payload.append(msg)

        elif type == "CHANNEL":
            for param in ["ChannelCapacity", "ChannelSize", "ChannelFillPercentage", "KafkaCommitTimer",
                          "KafkaEventGetTimer", "KafkaEventSendTimer", "Open", "Unhealthy"]:
                metric = key.replace("CHANNEL.", "flume.channel.") + "." + param
                value = float(res[key][param])
                msg = load(hostname, metric, ts, step, value, GAUGE, tags)
                payload.append(msg)

            for param in ["CheckpointBackupWriteErrorCount", "CheckpointWriteErrorCount",
                          "EventPutAttemptCount", "EventPutErrorCount", "EventPutSuccessCount",
                          "EventTakeAttemptCount", "EventTakeErrorCount", "EventTakeSuccessCount", "RollbackCounter"
                          ]:
                metric = key.replace("CHANNEL.", "flume.channel.") + "." + param
                value = float(res[key][param])
                msg = load(hostname, metric, ts, step, value, COUNTER, tags)
                payload.append(msg)

        elif type == "SINK":
            for param in ["KafkaEventSendTimer"]:
                metric = key.replace("SINK.", "flume.sink.") + "." + param
                value = float(res[key][param])
                msg = load(hostname, metric, ts, step, value, GAUGE, tags)
                payload.append(msg)

            for param in ["BatchCompleteCount", "BatchEmptyCount", "BatchUnderflowCount", "ChannelReadFail",
                          "ConnectionClosedCount", "ConnectionCreatedCount", "ConnectionFailedCount",
                          "EventDrainAttemptCount", "EventDrainSuccessCount", "EventWriteFail", "RollbackCount"]:
                metric = key.replace("SINK.", "flume.sink.") + "." + param
                value = float(res[key][param])
                msg = load(hostname, metric, ts, step, value, COUNTER, tags)
                payload.append(msg)

    print json.dumps(payload)
except Exception, e:
    print e
