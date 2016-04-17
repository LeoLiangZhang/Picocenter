#!/bin/bash

# DO NOT RUN WITH SUDO

# prerequisite
# 1. have a ssh key for gitlab.liangzhang.me 
#    liang: scp ~/.ssh/id_rsa_picocenter ubuntu@ec2-54-166-165-221.compute-1.amazonaws.com:.ssh/

# CHANGE THIS IF MOUNTED IN A DIFFERENT LOCATION, THIS IS THE DEFAULT FOR FIRST DISK ON AWS
BTRFS_DISK=/dev/xvdf
BTRFS_PARTITION=/dev/xvdf1

sudo mkdir /elasticity
sudo chown $USER /elasticity
cd /elasticity/

sudo apt-get update

# some tools
sudo apt-get install -y moreutils python-ujson rlwrap

# install NetfilterQueue and its Python bindings
apt-get install build-essential python-dev libnetfilter-queue-dev 
apt-get install python-dpkt # to parse IP packet
pip install NetfilterQueue

sudo apt-get install -y python-pip \
	build-essential pkg-config autoconf \
	libfuse-dev libboost-all-dev libattr1-dev \
	libcurl4-openssl-dev libssl-dev libxml2-dev \

# CRIU 
sudo apt-get install -y libprotobuf-dev libprotobuf-c-dev \
	protobuf-c-compiler protobuf-compiler python-protobuf \

# LXC 
sudo apt-get install -y libcap-dev libapparmor-dev \
	libselinux1-dev libseccomp-dev libgnutls-dev \
	liblua5.2-dev python3-dev \
	docbook2x doxygen \
	libvirt-bin

# cgmanager
sudo apt-get install -y libnih-dev libnih-dbus-dev libpam0g-dev

sudo pip install boto tornado msgpack-python pyzmq

# build PageServer
cd /elasticity
git clone git@github.com:LeoLiangZhang/Picocenter.git
cd /elasticity/Picocenter/fuse
make

# build CRIU
cd /elasticity
git clone git@github.com:LeoLiangZhang/criu.git
cd /elasticity/criu
git checkout litton_1_7
make
sudo make install
# criu doc depends on asciidoc. It is too big, I skip it.

# build cgmanager
cd /elasticity
git clone https://github.com/lxc/cgmanager.git
cd cgmanager/
git checkout tags/v0.38
autoreconf --install
./configure && make && sudo make install

# build LXC
cd /elasticity
git clone https://github.com/lxc/lxc.git
cd lxc
git checkout tags/lxc-1.1.3
autoreconf --install
./configure --enable-doc --enable-api-docs --enable-cgmanager
make && sudo make install

# Fix python paths
cd /usr/local/lib/python3/dist-packages
for i in *; do sudo ln -s $(pwd)/$i /usr/local/lib/python3.4/dist-packages/; done

cd ~
sudo ldconfig

# SETUP BTRFS DISK
sudo apt-get install -y btrfs-tools

sudo fdisk $BTRFS_DISK
# n p <enter> <enter> <enter> w

sudo mkfs.btrfs -f $BTRFS_PARTITION

sudo lxc-create -n alpine_base -t /usr/local/share/lxc/templates/lxc-alpine
#apk add nginx screen postfix lighttpd vsftpd bind unbound g++ make gcc gdb mysql python perl ruby php-common php-iconv php-json php-gd php-curl php-xml php-pgsql php-imap php-cgi fcgi php-pdo php-pdo_pgsql php-soap php-xmlrpc php-posix php-mcrypt php-gettext php-ldap php-ctype php-dom

sudo mkdir /mnt/btrfs
sudo mount $BTRFS_PARTITION /mnt/btrfs
sudo btrfs subvolume create /mnt/btrfs/alpine_base

cd /mnt/btrfs
sudo cp -pdRX /usr/local/var/lib/lxc/alpine_base .
sed -i "$ a\
$BTRFS_PARTITION\t/usr/local/var/lib/lxc\tbtrfs\tdefaults\t0 1" /etc/fstab

sudo mount /usr/local/var/lib/lxc
sudo btrfs property set -ts /usr/local/var/lib/lxc/alpine_base/ ro true

sudo mkdir /checkpoints
sudo mkdir /filesystems

