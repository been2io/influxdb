#!/usr/bin/env bash
v=v0.0.6
img=hub.byted.org/alarm/influxdb:$v
docker build -t $img .
docker push $img
