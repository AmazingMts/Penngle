#!/bin/bash

rm -rf classes
rm -rf lib/webserver.jar
rm -rf lib/kvs.jar
rm -rf lib/flame.jar

# Create necessary directories
mkdir -p classes
mkdir -p lib

# Set classpath with Lucene dependencies
CLASSPATH="lib/lucene-analysis-common-10.0.0.jar:lib/lucene-core-10.0.0.jar"

# Step 1: Compile all source files
echo "Compiling system files..."
javac -cp "${CLASSPATH}" \
    -d classes \
    src/cis5550/external/PorterStemmer.java \
    src/cis5550/flame/*.java \
    src/cis5550/generic/*.java \
    src/cis5550/jobs/*.java \
    src/cis5550/kvs/*.java \
    src/cis5550/tools/*.java \
    src/cis5550/webserver/*.java \
    src/cis5550/test/*java

# Step 2: Create system jar files
echo "Creating system jar files..."

# Create webserver.jar
jar cf lib/webserver.jar -C classes cis5550/webserver
# Create kvs.jar
jar cf lib/kvs.jar -C classes cis5550/kvs -C classes cis5550/tools -C classes cis5550/generic
# Create flame.jar
jar cf lib/flame.jar -C classes cis5550/flame -C classes cis5550/tools -C classes cis5550/generic

echo "System compilation complete!"