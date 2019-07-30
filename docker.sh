#!/usr/bin/env bash
v=v0.1.8
img=hub.byted.org/alarm/influxdb:$v
docker build -t $img .
docker push $img
