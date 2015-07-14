less $(head -n 1 $(ls -t1 shippable/testresults/*-output.txt | head -n 1) | sed -n 's/^.*Temp directory: //p')/logs/TabletServer*.out
