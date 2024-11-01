#!/usr/bin/env bash

COMMAND="docker-compose -f docker-compose.rc.yml"
if [ "$1" == "" ]; then
    $COMMAND up
else
    $COMMAND ${@}
fi

