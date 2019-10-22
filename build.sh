#!/bin/bash

rm -rf target/
mkdir target

cp -R buoy target/

cd target/
pip install requests -t ./
pip install pytz -t ./
pip install python-twitter -t ./
pip install https://files.pythonhosted.org/packages/19/7a/60bd79c5d79559150f8bba866dd7d434f0a170312e4d15e8aefa5faba294/matplotlib-3.1.1-cp37-cp37m-manylinux1_x86_64.whl -t ./
zip -r buoy-lambda.zip *

cd -