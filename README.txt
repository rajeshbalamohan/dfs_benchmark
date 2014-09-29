mvn clean package

Create test data:
=================
for i in `seq 1 1000`; do echo $i > ./$i.txt; done

hadoop dfs -put ./test /tmp/

To Run:
=======
specify the number of threads and the directory you would like to scan (recursive mode supported).
e.g following would spawn 60 threads and each of them would scan /tmp/test directory

java -cp /grid/0/hadoopConf/:./target/lib/*:./target/*: DFSBenchmark 60 /tmp/test/
