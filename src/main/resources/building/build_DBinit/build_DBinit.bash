# example: ./build_DBinit d4m_api_java-2.5.6-SNAPSHOT.jar libext
echo "1 is $1" #DO_REPLACE_JAR_NAME
echo "2 is $2" #libext/ for DO_REPLACE_MANY_LIBEXT_NAME
echo "3 is $3" #input file path : build_DBinit.m
echo "4 is $4" #output file path: DBinit.m
deps="`ls -1 $2`"
echo "" > "$4"
while read -u 10 p; do
	if echo "$p" | grep -q "DO_REPLACE_JAR_NAME"; then
		echo "$p" | perl -pwe "s/DO_REPLACE_JAR_NAME/$1/g;" >> "$4"
	elif echo "$p" | grep -q "DO_REPLACE_MANY_LIBEXT_NAME"; then
		while read -r line; do
			echo "$p" | perl -pwe "s/DO_REPLACE_MANY_LIBEXT_NAME/$line/g;" >> "$4"
		done <<< "$list"
	else
		echo "$p" >> "$4"
	fi		
done 10<"$3"
