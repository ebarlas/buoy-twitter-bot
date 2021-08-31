#!/bin/bash

rm -rf target/
mkdir target

cp -R buoy target/

cd target/
pip3 install requests -t ./
pip3 install pytz -t ./
pip3 install python-twitter -t ./
pip3 install matplotlib -t ./
zip -r buoy-lambda.zip *

cd -