#!/bin/ash

sed -i 's/need net/#need net/' /etc/init.d/postfix

# fix some postfix config issues in the container
echo 'root: root' > /etc/postfix/aliases
newaliases
mkdir /var/mail

rc-service postfix start
