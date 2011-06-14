#/bin/sh
######################################################################################################
# 
# Exports the differences between two revision numbers
#      Usage:  diffex [starting revision number]:[ending revision number] [url to respository]
#  
#      Example: export the differences from revision 702 to the HEAD of the repository
#          diffex 702:HEAD http://some/respository/trunk
# 
#      Requires: svn, awk, cut
#
# cmcnally 
# 11/19/2010
######################################################################################################

# Setup some variables
E_BADARGS=1;
SUCCESS=0;
counter=0;

# Argument check
if [ $# -ne 2 ] ; then

  # Print usages statement if not enough arguments are provided
  echo "Exports the differences between two revision numbers"
  echo "    Usage:  diffex [starting revision number]:[ending revision number] [url to respository]"
  echo ""
  echo "    Example: export the differences from revision 702 to the HEAD of the repository"
  echo "        diffex 702:HEAD http://some/respository/trunk"
  echo ""

  # Exit with the "bad arguments" error code
  exit $E_BADARGS
else

  # Get the list of differences
  diffs=(`svn diff $2 --summarize -r $1 | cut -b 8-`)

  # How many differences are there?
  echo "Found ${#diffs[*]} differences"
  echo "Exporting files: "

  # For Each file that's different between the revisions
  for i in ${diffs[*]};
  do 
 
    # Increment counter
    let counter=$counter+1

    # Parse the path from the url
    fullpath=`echo $i | cut -d / -f 9-`;
 
    # Seperate directories/filename
    dir=`echo $fullpath | awk -F"/" '{ for (i=1; i<NF; i++) printf $i"/" }'`;

    # Create the neccessary directories
    mkdir -p $dir 2>&1>&/dev/null;

    # Print what's being exported
    echo "  $i"

    # Export this file into the correct directory
    svn export $2/$fullpath $fullpath 2>&1>&/dev/null;
  done
  echo "Exported $counter files"
fi

exit $SUCCESS;
