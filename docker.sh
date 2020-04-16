#!/usr/bin/env bash
v=flux0.5.9
img=hub.byted.org/alarm/influxdb:$v
docker build -t $img .
docker push $img
