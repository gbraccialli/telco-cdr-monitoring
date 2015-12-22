echo "Stopping Demo Topology"
storm kill CDR

echo "Stopping Demo CDR Producer"
pgrep -f GenerateSampleCDR | xargs kill -9

echo "Stopping Demo Flume"
pgrep -f flume | xargs kill -9
