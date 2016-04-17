#!/bin/bash

flag=$1
# -d means debug, have verbose logging from fuse
# flag=-d
sudo umount /checkpoints/
sudo rm -rf /checkpoints/
sudo rm -rf /tmp/pico_cache/
sudo mkdir /checkpoints

ulimit -c unlimited

if [ -z "$WS_ACCESS_KEY" ] || [ -z "$WS_SECRET_KEY" ] ; then
    WS_ACCESS_KEY="YOUR AWS KEY"
    WS_SECRET_KEY="YOUR AWS SECRET"
fi

if [ -z "$PICO_BLOCK_SIZE" ] ; then
    PICO_BLOCK_SIZE=131072
fi

# Run FUSE page server in foreground
WS_ACCESS_KEY="$WS_ACCESS_KEY" WS_SECRET_KEY="$WS_SECRET_KEY" PICO_BLOCK_SIZE="$PICO_BLOCK_SIZE" /elasticity/fuse/main /checkpoints/ -f $flag
