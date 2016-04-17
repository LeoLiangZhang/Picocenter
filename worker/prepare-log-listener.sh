#!/bin/bash

# This script sets up iptables to send log messages to 127.0.0.1:12345

# The worker listens on this port to know when a warm picoprocess
# recieves an incoming packet so that it can restore it automatically

ROOT_DIR=/elasticity/
WORKER_DIR=$ROOT_DIR/worker/
IPTABLES_FILE=iptables_rsyslog.conf

echo 'Build iptables_helper'
cd $WORKER_DIR/iptables_helper 
sudo make clean && sudo make
cd ..
echo

if [ ! -f /etc/rsyslog.d ]; then
	echo 'Create symbolic link to' $IPTABLES_FILE
	sudo ln -s $WORKER_DIR/$IPTABLES_FILE \
	           /etc/rsyslog.d/$IPTABLES_FILE
fi

echo 'Restart rsyslog'
sudo service rsyslog restart

echo 'List dependencies'
ls -l $WORKER_DIR/iptables_helper/build/iptables_helper
ls -l /etc/rsyslog.d/$IPTABLES_FILE
sudo iptables -t nat -nvL
echo "DONE"

#sudo iptables -I FORWARD -p tcp -i eth0 -m state --state NEW,ESTABLISHED,RELATED -j ACCEPT
