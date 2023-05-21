#!/bin/bash

set -e

clear

for a in to_parquet build_from_file
do
    echo "`date` Testando $a"
    hatch run test tests/test_${a}.py
    echo "---------------------------------------------------"
    echo " "
done

