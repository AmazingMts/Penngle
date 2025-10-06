#!/bin/bash

####################################
# Clean up old files
rm -rf classes/*
rm -f *.jar
rm -r worker1

# Make sure classes directory exists
mkdir -p classes

# Compile all source files with Lucene dependencies
CLASSPATH="lib/lucene-analysis-common-10.0.0.jar:lib/lucene-core-10.0.0.jar"

# Main compilation
javac -cp "${CLASSPATH}" \
    -d classes \
    src/cis5550/external/PorterStemmer.java \
    src/cis5550/flame/*.java \
    src/cis5550/generic/*.java \
    src/cis5550/jobs/*.java \
    src/cis5550/kvs/*.java \
    src/cis5550/tools/*.java \
    src/cis5550/webserver/*.java \
    src/cis5550/test/KvsBenchmark.java

# Compile Indexer separately to ensure it's properly compiled with dependencies
javac -cp "${CLASSPATH}:classes" -d classes src/cis5550/jobs/Indexer.java

# Create manifest files for each JAR
echo "Class-Path: lib/lucene-analysis-common-10.0.0.jar lib/lucene-core-10.0.0.jar" > manifest.txt

# Create JARs with manifest
jar cfm kvsBenchmark.jar manifest.txt -C classes .
jar cfm crawler.jar manifest.txt -C classes .
jar cfm pagerank.jar manifest.txt -C classes .
jar cfm indexer.jar manifest.txt -C classes .

# Clean up manifest
rm manifest.txt

echo "Compilation complete. JARs created: crawler.jar, pagerank.jar, and indexer.jar"

#java -cp "lib/lucene-analysis-common-10.0.0.jar:lib/lucene-core-10.0.0.jar:classes" cis5550.kvs.Coordinator 8000

#java -cp "lib/lucene-analysis-common-10.0.0.jar:lib/lucene-core-10.0.0.jar:classes" cis5550.kvs.Worker 8001 worker1 localhost:8000

#java -cp "lib/lucene-analysis-common-10.0.0.jar:lib/lucene-core-10.0.0.jar:classes" cis5550.flame.Coordinator 9000 localhost:8000

#java -cp "lib/lucene-analysis-common-10.0.0.jar:lib/lucene-core-10.0.0.jar:classes" cis5550.flame.Worker 9001 localhost:9000

#java -cp "classes:crawler.jar" cis5550.flame.FlameSubmit localhost:9000 crawler.jar cis5550.jobs.Crawler http://simple.crawltest.401.cis5550.net

#java -cp "lib/lucene-analysis-common-10.0.0.jar:lib/lucene-core-10.0.0.jar:classes:indexer.jar" cis5550.flame.FlameSubmit localhost:9000 indexer.jar cis5550.jobs.Indexer

#java -cp "classes:pagerank.jar" cis5550.flame.FlameSubmit localhost:9000 pagerank.jar cis5550.jobs.PageRank 0.01

#java -cp "classes:pagerank.jar" cis5550.flame.FlameSubmit localhost:9000 kvsBenchmark.jar cis5550.test.KvsBenchmark