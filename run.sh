#!/bin/bash

cmd=$@
#  gdelttools/mongo-parallel-import.py
docker exec gdelt $cmd
