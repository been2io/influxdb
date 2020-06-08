#!/usr/bin/env bash
v=v0.5.2.patch2
img=hub.byted.org/alarm/influxdb:$v
docker build -t $img .
docker push $img
