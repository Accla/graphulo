# example: ./build_DBinit d4m_api_java-2.5.6-SNAPSHOT.jar libext
DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
a="${project.build.finalName}" #DO_REPLACE_JAR_NAME
b="$DIR/../../../${DBinit.path.libextFolder}" #libext/ for DO_REPLACE_MANY_LIBEXT_NAME
c="$DIR/${DBinit.path.template}" #input file path : build_DBinit.m
d="$DIR/../../../${DBinit.path.outputFile}" #output file path: DBinit.m
e="$DIR/../../../${DBinit.path.libextZip}" #location of libext zip archive to add result to
deps="`ls -1 $b`"
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
echo "${DBinit.path.outputFile} built successfully and placed inside ${DBinit.path.libextZip}"
