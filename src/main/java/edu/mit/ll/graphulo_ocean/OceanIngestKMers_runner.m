
% cp '/home/gridsan/dhutchison/gits/graphulo/src/main/java/edu/mit/ll/graphulo_ocean/OceanIngestKMers_runner.m' .
% eval(pRUN('OceanIngestKMers_runner',16,'grid&'))

%PARALLEL = 1;

opts = javaArray('java.lang.String',16);
opts(1) = java.lang.String('-listOfSamplesFile');
opts(2) = java.lang.String('/home/gridsan/dhutchison/gits/istc_oceanography/metadata/valid_samples_GA02_filenames_perm.csv');
opts(3) = java.lang.String('-everyXLines');
opts(4) = java.lang.String(num2str(Np));
opts(5) = java.lang.String('-startOffset');
opts(6) = java.lang.String(num2str(Pid));
opts(7) = java.lang.String('-K');
opts(8) = java.lang.String('11');
opts(9) = java.lang.String('-oTsampleDegree');
opts(10) = java.lang.String('oTsampleDegree');
opts(11) = java.lang.String('-txe1');
opts(12) = java.lang.String('classdb55');
opts(13) = java.lang.String('-oTsampleSeqRaw');
opts(14) = java.lang.String('oTsampleSeqRaw');
opts(15) = java.lang.String('-alsoIngestSmallerLex');
opts(15) = java.lang.String('-alsoIngestReverseComplement');

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

mymap = map([Np 1],{},0:Np-1);
X = zeros(Np,1,mymap);
%Xloc = local(X);

% sync
Xloc = -Pid
X = put_local(X, Xloc);
agg_all(X)

if Pid <> 0
  pause(10);
end

tic;
Xloc = edu.mit.ll.graphulo_ocean.OceanIngestKMers.executeNew(opts);
t = toc; fprintf('Total time is %d\n', t);

X = put_local(X, Xloc);
display(X);

%if Pid == 0
%  fprintf('Count is %d\n', nnz(X));
%  fprintf('Sum is   %d\n', sum(X));
%end
