#!/bin/bash
echo "Starting nginx to serve Ballista Scheduler web UI on port 80"
nohup nginx -g "daemon off;" &
/scheduler "$@"