%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% D4M: Dynamic Distributed Dimensional Data Model
% Architect: Dr. Jeremy Kepner (kepner@ll.mit.edu)
% Software Engineers: 
%   Mr. Craig McNally (cmcnally@ll.mit.edu)
%   Dr. Jeremy Kepner (kepner@ll.mit.edu)
%   Mr. Will Smith (will.smith@ll.mit.edu)
%   Mr. Chuck Yee (yee@ll.mit.edu)
%
% MIT Lincoln Laboratory
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% (c) <2010> Massachusetts Institute of Technology
% FOUO
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

INTRODUCTION

  D4M_API is set of D4M scripts that enable the storage
  and retrieval of associative arrays into a database.

REQUIREMENTS

  D4M (standalone)
  -Matlab (or GNU ctave 3.2.2+ with Java package 1.2.6+)

  D4M (w/database)
    -CLOUDBASE
      -Zookeeper
      -Hadoop File system
      -Cloudbase database
    -HBASE (TBD)
    -OTHERS (TBD)



INSTALLING AND RUNNING FULL OR LLONLY DISTRIBUTION:

  - Add the following to your ~/matlab/startup.m file (or ~/.octaverc file).

      addpath('<ParentDir>/d4m_api/matlab_src');  % Replace <ParentDir> with location of d4m_api.
      DBinit;    % Initalizes java path.  NOTE: Octave requires Java package be installed.

  - Edit matlab_src/DBinit.m file and find missing .jar files and put them in lib directory.

       The ExternalContrib file also contains brief description of the jars.

  - Edit d4m_api/matlab_src/TEST/DBsetup.m so it points to your Cloudbase server.

  - Start Matlab (or octave --traditional).

  - cd to d4m_api/matlab_src/TEST, and run any script ending in TEST.m

  - To run all tests type:

       Atest = runTESTdir('./')


INSTALLING LLONLY STUB DISTRIBUTION:

  - Assumes a LLONLY distribution has already been installed.

  - Unpack LLONLY STUB distribution on top of existing directory
    to get most recent changes to LLONLY distribution.


RUNNING ON MacOSX

  Same as above.

CREATING A FULL DISTRIBUTION:
  svn export <URL>
  ant package
  ant zip

CREATING A LLONLY DISTRIBUTION:
  ant ll_package
  ant zip


CREATING A LLONLY STUB DISTRIBUTION:

KNOWN ISSUES

  GNU Octave 3.2.2 / Java PKG 1.2.6
    matlab_src/runTESTdir only works if Octave is launched in TEST directory

