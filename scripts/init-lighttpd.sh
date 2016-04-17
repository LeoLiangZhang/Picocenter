#!/bin/ash

sed -i 's/need net/#need net/' /etc/init.d/lighttpd
rc-service lighttpd start
