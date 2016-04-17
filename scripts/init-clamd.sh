#!/bin/ash

ping -c 1 8.8.8.8
sed -i 's/need net/#need net/' /etc/init.d/clamd /etc/init.d/freshclam
rc-service clamd start
