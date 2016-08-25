
% cp '/home/gridsan/dhutchison/gits/graphulo/src/main/java/edu/mit/ll/graphulo_ocean/OceanDistanceCalc_runner.m' .

opts = javaArray('java.lang.String',10);
opts(1) = java.lang.String('-oTsampleSeqRaw');
opts(2) = java.lang.String('oTsampleSeqRaw');
opts(3) = java.lang.String('-oTsampleDegree');
opts(4) = java.lang.String('oTsampleDegree');
opts(5) = java.lang.String('-oTsampleDist');
opts(6) = java.lang.String('oTsampleDist');
opts(7) = java.lang.String('-txe1');
opts(8) = java.lang.String('classdb55');
opts(9) = java.lang.String('-sampleFilter');
opts(10) = java.lang.String(':,');

console = org.apache.log4j.ConsoleAppender();
console.setThreshold(org.apache.log4j.Level.INFO);
%console.setName(java.lang.String('console'));
console.activateOptions();
PATTERN = java.lang.String('%d [%p|%c|%C{1}] %m%n');
pl = org.apache.log4j.PatternLayout(PATTERN);
console.setLayout(pl);
rl = org.apache.log4j.Logger.getRootLogger();
rl.addAppender(console);

zl = org.apache.log4j.Logger.getLogger(java.lang.String('org.apache.zookeeper'));
zconsole = org.apache.log4j.ConsoleAppender();
zconsole.setThreshold(org.apache.log4j.Level.WARN);
%zconsole.setName(java.lang.String('zconsole'));
zconsole.activateOptions();
zl.addAppender(zconsole);
zl.setAdditivity(false);

javaaddpath /home/gridsan/dhutchison/gits/graphulo/target/graphulo-1.0.0-SNAPSHOT-all.jar
%javaaddpath /home/dhutchis/gits/graphulo/target/graphulo-1.0.0-SNAPSHOT-all.jar

edu.mit.ll.graphulo_ocean.OceanDistanceCalc.main(opts)




