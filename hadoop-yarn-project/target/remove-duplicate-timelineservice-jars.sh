yarnjardir="/home/hadoop/project/hadoop-3.1.1-src/hadoop-yarn-project/target/hadoop-yarn-project-3.1.1/share/hadoop/yarn"
                      yarnlibdir="${yarnjardir}/lib/"
                      cd "${yarnjardir}/timelineservice/lib"
                      for jar in `ls`
                      do
                        if [ -f "${yarnlibdir}""${jar}" ];then
                          rm -rf ${jar}
                          echo "Removing duplicate jar $jar from share/hadoop/yarn/timelineservice/lib folder"
                        fi
                      done