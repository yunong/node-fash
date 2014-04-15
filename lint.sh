#!/bin/bash

git submodule update

cd ./deps/javascriptlint
make install
cd ../..

./deps/javascriptlint/build/install/jsl --conf ./tools/jsl.node.conf \
    ./lib/*.js ./lib/backend/*.js ./bin/*.js

./deps/jsstyle/jsstyle -f ./tools/jsstyle.conf ./*.js ./bin/*.js ./test/*.js
