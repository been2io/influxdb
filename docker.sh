#!/usr/bin/env bash
v=m0.6.0
img=hub.byted.org/alarm/influxdb:$v
docker build -t $img .
docker push $img
