#!/bin/bash

set -e

hatch clean
hatch build
hatch run python3 main.py
echo "*******************************************************"
cat timeseries.log

