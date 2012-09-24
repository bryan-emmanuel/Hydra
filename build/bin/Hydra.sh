#!/bin/sh
##############################################################
# chkconfig: 345 98 98
### BEGIN INIT INFO
# Provides: Hydra
# Required-Start: $local_fs $network $remote_fs
# Required-Stop: $local_fs $network $remote_fs
# Default-Start:  3 4 5
# Default-Stop: 0 1 6
# Short-Description: start and stop Hydra
# Description: Hydra is a database connection service
### END INIT INFO

HYDRA_HOME=/usr/local/hydra
HYDRA_SERVICE=com.piusvelte.hydra.HydraService
HYDRA_USER=hydra

TMP_DIR=/var/tmp
PID_FILE=/var/run/jsvc.pid


CLASSPATH=$HYDRA_HOME/bin/Hydra.jar


case "$1" in
  start)
    #-user $HYDRA_USER \
    echo $"Starting Hydra..."
    cd $HYDRA_HOME && \
    $HYDRA_HOME/bin/jsvc \
    -home $JAVA_HOME \
    -Djava.io.tmpdir=$TMP_DIR \
    -wait 10 \
    -pidfile $PID_FILE \
    -outfile $HYDRA_HOME/logs/hydra.out \
    -errfile '&1' \
    -cp $CLASSPATH \
    $HYDRA_SERVICE
    #
    # To get a verbose JVM
    #-verbose \
    # To get a debug of jsvc.
    #-debug \
    exit $?
  ;;


  stop)
  echo $"Stopping Hydra..."
    $HYDRA_HOME/bin/jsvc \
    -stop \
    -pidfile $PID_FILE \
    $HYDRA_SERVICE
    exit $?
  ;;


  restart)
    $0 stop
    $0 start
    exit $?
  ;;


  *)
    echo "Usage: hydra {start|stop}"
    exit 1;;
esac
