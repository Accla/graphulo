
% cp '/home/gridsan/dhutchison/gits/graphulo/src/main/java/edu/mit/ll/graphulo_ocean/OceanIngestKMers_runner.m' .
% eval(pRUN('OceanIngestKMers_runner',16,{'txe1'}))

%PARALLEL = 1;

opts = javaArray('java.lang.String',14);
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
opts(12) = java.lang.String('classdb54');
opts(13) = java.lang.String('-oTsampleSeqRaw');
opts(14) = java.lang.String('oTsampleSeqRaw');

edu.mit.ll.graphulo_ocean.OceanIngestKMers.main(opts)




