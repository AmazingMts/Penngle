::!/bin/bash

::Clean up old files
start cmd /c rmdir /s /q worker1

::Make sure classes directory exists
mkdir -p classes

::Make sure worker1

::Compile all source files
javac -d classes -cp "integrated/lib/lucene-analysis-common-10.0.0.jar;integrated/lib/lucene-core-10.0.0.jar" integrated/src/cis5550/frontend/*.java integrated/src/cis5550/external/PorterStemmer.java integrated/src/cis5550/flame/*.java integrated/src/cis5550/generic/*.java integrated/src/cis5550/kvs/*.java integrated/src/cis5550/tools/*.java integrated/src/cis5550/webserver/*.java integrated/src/cis5550/test/*.java integrated/src/cis5550/jobs/*.java

::Create PageRank JAR
jar cf pagerank.jar -C classes ./cis5550/jobs/PageRank.class

::Create Indexer JAR
jar cf indexer.jar -C classes ./cis5550/jobs/Indexer.class

::Create Crawler JAR
jar cf crawler.jar -C classes ./cis5550/jobs/Crawler.class

echo "Compilation complete"
