#!/bin/bash

cd ./deps/javascriptlint
make install
cd ../..

./deps/javascriptlint/build/install/jsl --conf ./tools/jsl.node.conf \
    ./lib/*.js ./lib/backend/*.js ./bin/*.js
