#!/bin/bash

set -e

echo 'Removendo logs...'
for a in `find . -name '*.log'`
do
  echo "rm $a"
  rm $a
done

echo 'Removendo arquivos de build...'

set -x
rm -r dist/*
set +x

