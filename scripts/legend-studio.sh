#! /usr/bin/env sh

########################################
# Start up script for legend engine DEV
# author: Antoine Amend
# tested: AWS EC2 REHL X.LARGE
########################################

LEGEND_HOME="$( cd -- "$(dirname "$0")" >/dev/null 2>&1 ; pwd -P )"
STUDIO_HOME="$LEGEND_HOME/legend-studio"
IP=`curl http://checkip.amazonaws.com 2>/dev/null`

case "$1" in
start)
  cd $STUDIO_HOME
  yarn dev > $LEGEND_HOME/legend-studio.log 2>&1 &
  echo $!>${LEGEND_HOME}/legend-studio.pid
  echo "Visit http://${IP}:8080"
  ;;
stop)
   kill `cat ${LEGEND_HOME}/legend-studio.pid`
   # belt
   rm ${LEGEND_HOME}/legend-studio.pid
   # braces
   ps -ef | grep legend-studio | awk '{print $2}' | xargs kill -9 2>/dev/null
   ;;
restart)
   $0 stop
   $0 start
   ;;
status)
   if [ -e ${LEGEND_HOME}/legend-studio.pid ]; then
      echo legend studio is running, pid=`cat ${LEGEND_HOME}/legend-studio.pid`
      echo "Visit http://${IP}:8080"
   else
      echo legend studio is NOT running
      exit 1
   fi
   ;;
*)
   echo "Usage: $0 {start|stop|status|restart}"
esac

exit 0
