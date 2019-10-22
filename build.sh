#!/bin/bash

rm -rf target/
mkdir target

cp -R buoy target/

cd target/
pip install requests -t ./
pip install pytz -t ./
pip install python-twitter -t ./
pip install numpy -t ./
pip install matplotlib -t ./
zip -r buoy-lambda.zip *

cd -