#!/usr/bin/env bash
v=flux0.9.0p1
img=hub.byted.org/alarm/influxdb:$v
docker build -t $img .
docker push $img
