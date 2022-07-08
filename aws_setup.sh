#!/bin/bash

# Initial setup
apt-get -y update
apt-get -y install \
wget \
git \
gcc-4.8 \
g++-4.8 \
g++ \
cmake \
libboost-context-dev \
libboost-dev \
doxygen \
transfig

# Build stage
export BUILD_ROOT=/opt
export TARGET_DIR=/opt/simgrid
export CLONE_DIR=simgrid-313
export SIMGRID_URL=https://framagit.org/simgrid/simgrid.git
export SIMGRID_VERSION=v3_13

## Clean up and prepare
cd ${BUILD_ROOT}
rm -f ${TARGET_DIR}
ln -s ${CLONE_DIR} ${TARGET_DIR}

## Clone repository
git clone -b ${SIMGRID_VERSION} --single-branch ${SIMGRID_URL} ${CLONE_DIR}

## Compile SimGrid
cd ${TARGET_DIR}
cmake -DBUILD_SHARED_LIBS=OFF -DCMAKE_INSTALL_PREFIX=${TARGET_DIR}
make
make check
make install

## Make libraries accessible
ln -s ${TARGET_DIR}/lib/libsimgrid.so /usr/lib/libsimgrid.so
ln -s ${TARGET_DIR}/lib/libsimgrid.so.3.13 /usr/lib/libsimgrid.so.3.13

# Install miniconda
wget https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh -O ~/miniconda.sh
bash ~/miniconda.sh -b -p $HOME/miniconda

eval "$(/home/ubuntu/miniconda/bin/conda shell.bash hook)"
conda init
chown -R ubuntu:ubuntu /home/ubuntu/miniconda # fix permissions

# Configuring git
git config --global user.name "Lucas de Sousa Rosa"
git config --global user.email "roses.lucas404@gmail.com"