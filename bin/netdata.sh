#!/usr/bin/env bash

# the frequency to send info to netdata
# passed by netdata as the first parameter
update_every="${1-1}"

socat ABSTRACT-LISTEN:/tmp/netdata,fork,reuseaddr SYSTEM:"echo \"START ${update_every}\" ",ignoreeof\!\!STDOUT

