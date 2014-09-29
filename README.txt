mvn clean package

To Run:
=======
specify the number of threads and the directory you would like to scan (recursive mode supported).
e.g following would spawn 60 threads and each of them would scan /tmp/yarn directory

java -cp /grid/0/hadoopConf/:./target/lib/*:./target/*: DFSBenchmark 60 /tmp/yarn/
