#!/usr/bin/env bash
v=lock0.6.2
img=hub.byted.org/alarm/influxdb:$v
docker build -t $img .
docker push $img
