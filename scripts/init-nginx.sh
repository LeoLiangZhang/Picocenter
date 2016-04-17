#!/bin/ash

sed -i 's/need net/#need net/' /etc/init.d/nginx
rc-service nginx start
