# example: ./build_DBinit d4m_api_java-2.5.6-SNAPSHOT.jar libext
echo "1 is $1" #DO_REPLACE_JAR_NAME
echo "2 is $2" #libext/ for DO_REPLACE_MANY_LIBEXT_NAME
deps="`ls -1 $2`"
echo "" > DBinit.m
while read -u 10 p; do
	if echo "$p" | grep -q "DO_REPLACE_JAR_NAME"; then
		echo "$p" | perl -pwe "s/DO_REPLACE_JAR_NAME/$1/g;" >> DBinit.m
	elif echo "$p" | grep -q "DO_REPLACE_MANY_LIBEXT_NAME"; then
		while read -r line; do
			echo "$p" | perl -pwe "s/DO_REPLACE_MANY_LIBEXT_NAME/$line/g;" >> DBinit.m
		done <<< "$list"
	else
		echo "$p" >> DBinit.m
	fi		
done 10<"build_DBinit.m"
