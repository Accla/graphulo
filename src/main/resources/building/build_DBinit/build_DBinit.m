%DBinit: Set up path for D4M binding to databases.
%Database user function.
%  Usage:
%    DBinit
%  Inputs:
%    
%  Outputs:
%    modifies java path

% Get directory that this file is in.
d4m_home = fileparts(fileparts(mfilename('fullpath')));


if 1

if ispc
 fd = '\';
else
 fd = '/';
end

% USER: Add external files *NOT* included in LLONLY distribution.
% Find the files and put them in lib or change these
% entries to point to these files.

% ****  IMPORTANT NOTE ****
% If you will use accumulo, check that libthrift-0.6.1.jar is list before thrift-0.3.jar
% Vice versa, if you will use cloudbase, list thrift-0.3.jar before libthrift-0.6.1.jar.
if exist('OCTAVE_VERSION','builtin')
   % Add files included in LLONLY distribution.
   javaaddpath([d4m_home fd 'lib' fd 'DO_REPLACE_JAR_NAME']);

   javaaddpath([d4m_home fd 'libext' fd 'DO_REPLACE_MANY_LIBEXT_NAME']);

else
   % MATLAB one line version (faster than adding individually) 
   %Common jars 
  javaaddpath({ ...
      [d4m_home fd 'lib' fd 'DO_REPLACE_JAR_NAME'] ...
	  , [d4m_home fd 'lib' fd 'DO_REPLACE_MANY_LIBEXT_NAME'] ...
	  });
end

clear d4m_home fd

end
