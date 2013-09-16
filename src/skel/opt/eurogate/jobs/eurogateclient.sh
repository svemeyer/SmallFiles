export EUROBASE=/Users/patrick/eurogate/classes
export CLASSPATH=$EUROBASE/eurogate.jar:$EUROBASE/cells.jar 
java eurogate.gate.EuroSyncClient -host=dcache-hsm.desy.de -port=28000 -debug  write /etc/group all

