#!/bin/bash
# example: ./build_DBinit d4m_api_java-2.5.6-SNAPSHOT.jar libext
#DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
a="$1" #"${project.build.finalName}.jar" #DO_REPLACE_JAR_NAME
b="$2" #"${project.build.directory}/${DBinit.path.libextFolder}" #libext/ for DO_REPLACE_MANY_LIBEXT_NAME
c="$3" #"${DBinit.path.mpath}" #input file path : build_DBinit.m
d="$4" #"${project.build.directory}/${DBinit.path.outputFile}" #output file path: DBinit.m
e="$5" #"${project.build.directory}/${DBinit.path.libextZip}" #location of libext zip archive to add result to
deps="`ls -1 ${b}`"
echo "" > "$d"
while read -u 10 p; do
	if echo "$p" | grep -q "DO_REPLACE_JAR_NAME"; then
		echo "$p" | perl -pwe "s/DO_REPLACE_JAR_NAME/$a/g;" >> "$d"
	elif echo "$p" | grep -q "DO_REPLACE_MANY_LIBEXT_NAME"; then
		while read -r line; do
			echo "$p" | perl -pwe "s/DO_REPLACE_MANY_LIBEXT_NAME/$line/g;" >> "$d"
		done <<< "$deps"
	else
		echo "$p" >> "$d"
	fi		
done 10<"$c"
zip -g -j "$e" "$d"
echo "${4##*/} built successfully and placed inside $e"
