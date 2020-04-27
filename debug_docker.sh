#!/usr/bin/env bash
v=debug0.6.9.3
img=hub.byted.org/alarm/influxdb:$v
docker build -f Dockerfile_debug -t $img .
docker push $img
