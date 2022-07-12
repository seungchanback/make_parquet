#!/bin/bash

sudo apt update
sudo apt install python3-pip
pip install -r requirements.txt
python3 ./make_parquet.py