#!/usr/bin/env bash
v=flux0.9.0
img=hub.byted.org/alarm/influxdb:$v
docker build -t $img .
docker push $img
