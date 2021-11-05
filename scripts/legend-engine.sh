#! /usr/bin/env sh

########################################
# Start up script for legend engine DEV
# author: Antoine Amend
# tested: AWS EC2 REHL X.LARGE
########################################

LEGEND_HOME="$( cd -- "$(dirname "$0")" >/dev/null 2>&1 ; pwd -P )"
ENGINE_HOME="$LEGEND_HOME/legend-engine"
ENGINE_VERSION=`cat $ENGINE_HOME/.version`
IP=`curl http://checkip.amazonaws.com 2>/dev/null`

case "$1" in
start)
  java -cp \
    ${LEGEND_HOME}/legend-spark.jar:${ENGINE_HOME}/legend-engine-server/target/legend-engine-server-${ENGINE_VERSION}-shaded.jar \
    org.finos.legend.engine.server.Server \
    server \
    ${LEGEND_HOME}/legend-engine.json > ${LEGEND_HOME}/legend-engine.log 2>&1 &
  echo $!>${LEGEND_HOME}/legend-engine.pid
  echo "Visit http://${IP}:6060"
  ;;
stop)
   kill `cat ${LEGEND_HOME}/legend-engine.pid`
   # belt
   rm ${LEGEND_HOME}/legend-engine.pid
   # braces
   ps -ef | grep legend-engine | awk '{print $2}' | xargs kill -9 2>/dev/null
   ;;
restart)
   $0 stop
   $0 start
   ;;
status)
   if [ -e ${LEGEND_HOME}/legend-engine.pid ]; then
      echo legend engine is running, pid=`cat ${LEGEND_HOME}/legend-engine.pid`
      echo "Visit http://${IP}:6060"
   else
      echo legend engine is NOT running
      exit 1
   fi
   ;;
*)
   echo "Usage: $0 {start|stop|status|restart}"
esac

exit 0
