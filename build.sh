#!/bin/bash

rm -rf target/
mkdir target

cp -R buoy target/

cd target/
pip install requests -t ./
pip install pytz -t ./
pip install python-twitter -t ./
pip install https://files.pythonhosted.org/packages/93/f8/518fb0bb89860eea6ff1b96483fbd9236d5ee991485d0f3eceff1770f654/kiwisolver-1.1.0-cp37-cp37m-manylinux1_x86_64.whl -t ./
pip install https://files.pythonhosted.org/packages/00/4a/e34fce8f18c0e052c2b49f1b3713469d855f7662d58ae2b82a88341e865b/numpy-1.17.3-cp37-cp37m-manylinux1_x86_64.whl -t ./
pip install https://files.pythonhosted.org/packages/19/7a/60bd79c5d79559150f8bba866dd7d434f0a170312e4d15e8aefa5faba294/matplotlib-3.1.1-cp37-cp37m-manylinux1_x86_64.whl -t ./
zip -r buoy-lambda.zip *

cd -