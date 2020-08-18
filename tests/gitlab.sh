#!/usr/bin/env sh

# Base requirements
apt install git build-tools python3-dev python3-pip libyajl2 mariadb-server mariadb-client

python setup.py deps
