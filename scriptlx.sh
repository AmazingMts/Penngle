#!/bin/bash

# Configuration
KVS_WORKERS=1
FLAME_WORKERS=1
KVS_COORD_PORT=8000
FLAME_COORD_PORT=9000

# Function for cleanup
function cleanup_processes() {
    echo "Cleaning up existing processes..."
    pkill -f "cis5550.kvs.Coordinator"
    pkill -f "cis5550.kvs.Worker"
    pkill -f "cis5550.flame.Coordinator"
    pkill -f "cis5550.flame.Worker"
    pkill -f "cis5550.jobs.Query"
    sleep 2
    echo "Cleanup complete"
}

# Clean up
cleanup_processes
rm -rf worker* *.jar run_*.sh

## Set classpath
#CLASSPATH="lib/lucene-analysis-common-10.0.0.jar:lib/lucene-core-10.0.0.jar:lib/webserver.jar:lib/kvs.jar:lib/flame.jar:classes"

# 获取当前目录的绝对路径
CURRENT_DIR=$(pwd)

# 设置CLASSPATH
CLASSPATH="${CURRENT_DIR}/lib/lucene-analysis-common-10.0.0.jar:${CURRENT_DIR}/lib/lucene-core-10.0.0.jar:${CURRENT_DIR}/lib/webserver.jar:${CURRENT_DIR}/lib/kvs.jar:${CURRENT_DIR}/lib/flame.jar:${CURRENT_DIR}/classes"

# Compile and create jars
echo "Creating job jars..."
#find src/cis5550/jobs -name "*.java" | xargs javac -cp "${CLASSPATH}" -d classes
javac -cp "${CLASSPATH}" -d classes src/cis5550/jobs/*.java

# Create manifest
echo "Class-Path: ${CLASSPATH}" > manifest.txt

# Create job jars
jar cfm indexer.jar manifest.txt -C classes cis5550/jobs/Indexer.class cis5550/external/PorterStemmer.class
jar cfm pagerank.jar manifest.txt -C classes cis5550/jobs/PageRank.class
jar cfm crawler.jar manifest.txt -C classes cis5550/jobs/Crawler.class

# Create startup scripts
echo "Creating startup scripts..."

# KVS Coordinator script
cat > run_kvs_coordinator.sh << 'EOF'
#!/bin/bash
java -cp "${CLASSPATH}" cis5550.kvs.Coordinator ${KVS_COORD_PORT}
EOF

# Flame Coordinator script
cat > run_flame_coordinator.sh << 'EOF'
#!/bin/bash
java -cp "${CLASSPATH}" cis5550.flame.Coordinator ${FLAME_COORD_PORT} localhost:${KVS_COORD_PORT}
EOF

# Create worker directories and scripts
for ((i=1; i<=KVS_WORKERS; i++)); do
    mkdir -p worker$i
    cat > run_kvs_worker_$i.sh << EOF
#!/bin/bash
java -cp "${CLASSPATH}" cis5550.kvs.Worker $((KVS_COORD_PORT + i)) worker$i localhost:${KVS_COORD_PORT}
EOF
done

for ((i=1; i<=FLAME_WORKERS; i++)); do
    cat > run_flame_worker_$i.sh << EOF
#!/bin/bash
java -cp "${CLASSPATH}" cis5550.flame.Worker $((FLAME_COORD_PORT + i)) localhost:${FLAME_COORD_PORT}
EOF
done

# Make scripts executable
chmod +x run_*.sh

# Start services
echo "Starting services..."

# Start KVS Coordinator
echo "Starting KVS Coordinator..."
./run_kvs_coordinator.sh &
sleep 5

# Start KVS Workers
echo "Starting KVS Workers..."
for ((i=1; i<=KVS_WORKERS; i++)); do
    ./run_kvs_worker_$i.sh &
    echo "Started KVS Worker $i"
done
sleep 10

# Verify KVS worker registration
echo "Verifying KVS workers..."
wget -q --spider http://localhost:${KVS_COORD_PORT}/workers || {
    echo "Warning: KVS workers might not be ready"
    sleep 5
}

# Start Flame Coordinator
echo "Starting Flame Coordinator..."
./run_flame_coordinator.sh &
sleep 5

# Start Flame Workers
echo "Starting Flame Workers..."
for ((i=1; i<=FLAME_WORKERS; i++)); do
    ./run_flame_worker_$i.sh &
    echo "Started Flame Worker $i"
done
sleep 5

# Start Query Service
echo "Starting Query Service..."
max_retries=5
retry_count=0
while [ $retry_count -lt $max_retries ]; do
    java -cp ${CLASSPATH}:classes cis5550.jobs.Query 8100 localhost:${KVS_COORD_PORT} &
    if [ $? -eq 0 ]; then
        echo "Query Service started successfully"
        break
    fi
    retry_count=$((retry_count + 1))
    echo "Retrying Query Service start (attempt $retry_count of $max_retries)..."
    sleep 5
done

# Print system info
echo "System started successfully!"
echo ""
echo "Available services:"
echo "- KVS Coordinator: http://localhost:${KVS_COORD_PORT}"
echo "- Flame Coordinator: http://localhost:${FLAME_COORD_PORT}"
echo "- Query Service: http://localhost:8100"
echo ""
echo "Use the following commands for jobs:"
echo "java -cp ${CLASSPATH}:crawler.jar cis5550.flame.FlameSubmit localhost:${FLAME_COORD_PORT} crawler.jar cis5550.jobs.Crawler [URL]"
echo "java -cp ${CLASSPATH}:indexer.jar cis5550.flame.FlameSubmit localhost:${FLAME_COORD_PORT} indexer.jar cis5550.jobs.Indexer"
echo "java -cp ${CLASSPATH}:pagerank.jar cis5550.flame.FlameSubmit localhost:${FLAME_COORD_PORT} pagerank.jar cis5550.jobs.PageRank [THRESHOLD]"
echo "java -cp  lib/lucene-analysis-common-10.0.0.jar:lib/lucene-core-10.0.0.jar:lib/webserver.jar:lib/kvs.jar:lib/flame.jar:classes:kvsBenchmark.jar cis5550.flame.FlameSubmit localhost:9000 kvsBenchmark.jar cis5550.test.KvsBenchmark"