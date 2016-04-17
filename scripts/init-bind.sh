#!/bin/ash

# liang: fix bind/named config

cat >/etc/bind/named.conf <<EOF
// This file is copied from /etc/bind/named.conf.authoritative

options {
	directory "/var/bind";

	listen-on { any; };
	listen-on-v6 { none; };

	allow-transfer {
		none;
	};

	pid-file "/var/run/named/named.pid";
	allow-recursion { none; };
	recursion no;
};

zone "priv.io" IN {
	type master;
	file "/etc/bind/priv.io.zone";
};
EOF

cat >/etc/bind/priv.io.zone <<EOF
; liang: borrow this zone from priv.io :)
;
; BIND data file for local loopback interface
;
;\$TTL	604800
\$TTL	86400
; TTL valid for a day
priv.io.	IN	SOA	ns.priv.io. ns2.priv.io. (
				      2		; Serial
				 604800		; Refresh
				  86400		; Retry
				2419200		; Expire
				 604800 )	; Negative Cache TTL
;
priv.io.	IN	NS	ns.priv.io.
priv.io.	IN	A	129.10.115.149
ns		IN	A	129.10.115.149
ns2		IN	A	129.10.115.149
www		IN	CNAME	priv.io.

EOF

# bind is called named as its binary name 
sed -i 's/need net/#need net/' /etc/init.d/named 
rc-service named start
