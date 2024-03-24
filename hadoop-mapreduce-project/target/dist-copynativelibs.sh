LIB_DIR="/home/hadoop/project/hadoop-3.1.1-src/hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-nativetask/target/native/target/usr/local/lib"
                      if [ -d ${LIB_DIR} ] ; then
                        TARGET_DIR="/home/hadoop/project/hadoop-3.1.1-src/hadoop-mapreduce-project/target/hadoop-mapreduce-3.1.1/lib/native"
                        mkdir -p ${TARGET_DIR}
                        cp -R ${LIB_DIR}/lib* ${TARGET_DIR}
                      fi