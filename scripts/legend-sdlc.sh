#! /usr/bin/env sh

########################################
# Start up script for legend engine DEV
# author: Antoine Amend
# tested: AWS EC2 REHL X.LARGE
########################################

LEGEND_HOME="$( cd -- "$(dirname "$0")" >/dev/null 2>&1 ; pwd -P )"
SDLC_HOME="$LEGEND_HOME/legend-sdlc"
SDLC_VERSION=`cat $SDLC_HOME/.version`
IP=`curl http://checkip.amazonaws.com 2>/dev/null`

case "$1" in
start)
  java -cp \
    ${SDLC_HOME}/legend-sdlc-server/target/legend-sdlc-server-${SDLC_VERSION}-shaded.jar \
    org.finos.legend.sdlc.server.LegendSDLCServer \
    server \
    ${LEGEND_HOME}/legend-sdlc.yaml > ${LEGEND_HOME}/legend-sdlc.log 2>&1 &
  echo $!>${LEGEND_HOME}/legend-sdlc.pid
  echo "Visit http://${IP}:7070/api/swagger"
  ;;
stop)
   kill `cat ${LEGEND_HOME}/legend-sdlc.pid`
   # belt
   rm ${LEGEND_HOME}/legend-sdlc.pid
   # braces
   ps -ef | grep legend-sdlc | awk '{print $2}' | xargs kill -9 2>/dev/null
   ;;
restart)
   $0 stop
   $0 start
   ;;
status)
   if [ -e ${LEGEND_HOME}/legend-sdlc.pid ]; then
      echo legend sdlc is running, pid=`cat ${LEGEND_HOME}/legend-sdlc.pid`
      echo "Visit http://${IP}:7070/api/swagger"
   else
      echo legend sdlc is NOT running
      exit 1
   fi
   ;;
*)
   echo "Usage: $0 {start|stop|status|restart}"
esac

exit 0
