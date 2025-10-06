# Penngle Search Engine

A distributed search engine implementation built with Java, featuring web crawling, indexing, PageRank algorithm, and distributed computing capabilities.

## 🚀 Features

- **Distributed Web Crawler** - Multi-threaded web crawling with robots.txt support
- **Advanced Indexing** - Inverted index with stemming and word position tracking
- **PageRank Algorithm** - Enhanced convergence criteria for better ranking
- **Distributed Computing** - Custom Flame framework for large-scale data processing
- **Key-Value Store (KVS)** - Distributed storage system for crawled data
- **Web Interface** - Clean search interface with HTTPS support
- **Query Processing** - Fast search with relevance scoring

## 🏗️ Architecture

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Web Crawler   │───▶│   Indexer       │───▶│   PageRank      │
└─────────────────┘    └─────────────────┘    └─────────────────┘
         │                       │                       │
         ▼                       ▼                       ▼
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   KVS Storage   │    │   Flame Compute │    │   Query Engine  │
└─────────────────┘    └─────────────────┘    └─────────────────┘
         │                       │                       │
         └───────────────────────┼───────────────────────┘
                                 ▼
                    ┌─────────────────┐
                    │  Web Interface  │
                    └─────────────────┘
```

## 📋 Prerequisites

- Java Runtime Environment (JRE) 8 or higher
- Required JAR files in the `lib` directory:
  - lucene-analysis-common-10.0.0.jar
  - lucene-core-10.0.0.jar

## 🚀 Quick Start

### Option 1: Automated Setup (Recommended)

1. Make the startup script executable:
```bash
chmod +x scriptlx.sh
```

2. Run the complete system:
```bash
./scriptlx.sh
```

### Option 2: Manual Setup

Start each component in separate terminal windows:

#### 1. KVS Coordinator
```bash
java -cp "lib/lucene-analysis-common-10.0.0.jar:lib/lucene-core-10.0.0.jar:classes" cis5550.kvs.Coordinator 8000
```

#### 2. KVS Worker
```bash
java -cp "lib/lucene-analysis-common-10.0.0.jar:lib/lucene-core-10.0.0.jar:classes" cis5550.kvs.Worker 8001 worker1 localhost:8000
```

#### 3. Flame Coordinator
```bash
java -cp "lib/lucene-analysis-common-10.0.0.jar:lib/lucene-core-10.0.0.jar:classes" cis5550.flame.Coordinator 9000 localhost:8000
```

#### 4. Flame Worker
```bash
java -cp "lib/lucene-analysis-common-10.0.0.jar:lib/lucene-core-10.0.0.jar:classes" cis5550.flame.Worker 9001 localhost:9000
```

#### 5. Query Service
```bash
java -cp "lib/lucene-analysis-common-10.0.0.jar:lib/lucene-core-10.0.0.jar:classes" cis5550.jobs.Query 8100 localhost:8000
```

#### 6. Web Interface
```bash
java -cp "lib/lucene-analysis-common-10.0.0.jar:lib/lucene-core-10.0.0.jar:classes" cis5550.frontend.Frontend 8443 localhost:8100
```

## 🔧 Configuration

### Port Configuration
- KVS Coordinator: 8000
- KVS Worker: 8001
- Flame Coordinator: 9000
- Flame Worker: 9001
- Query Service: 8100
- Web Interface: 8443 (HTTPS)

### Script Configuration
Modify the number of workers in `scriptlx.sh`:
```bash
KVS_WORKERS=1
FLAME_WORKERS=1
```

## 📁 Project Structure

```
Penngle/
├── src/cis5550/           # Source code
│   ├── jobs/              # Core jobs (Crawler, Indexer, PageRank, Query)
│   ├── kvs/               # Key-Value Store implementation
│   ├── flame/             # Distributed computing framework
│   ├── webserver/         # Web server components
│   ├── frontend/          # Frontend interface
│   └── tools/             # Utility classes
├── lib/                   # JAR dependencies
├── job/                   # Compiled job JARs
├── data/                  # Crawled data storage
├── page/                  # Web interface files
└── *.sh                   # Startup scripts
```

## 🛠️ Development

### Compiling
```bash
./compile.sh
```

### Running Tests
```bash
java -cp "lib/*:classes" cis5550.test.KvsBenchmark
java -cp "lib/*:classes" cis5550.test.JobsBenchmark
```

## 🔍 Usage

1. Start the system using the startup script
2. Open your browser and navigate to `https://localhost:8443`
3. Enter your search query
4. View ranked search results

## 🐛 Troubleshooting

- **Port conflicts**: Ensure all required ports are available
- **JAR files**: Verify all dependencies are in the `lib` directory
- **Memory issues**: Increase JVM heap size if needed
- **Network issues**: Check firewall settings for required ports

## 📝 Notes

- The KVS Coordinator must be started first
- Each worker needs a unique port for multi-worker setups
- The system uses HTTPS for the web interface
- Crawled data is stored in the `data/` directory

## 🤝 Contributing

This is a distributed search engine implementation for educational purposes. Feel free to explore the code and learn about distributed systems, search algorithms, and web crawling techniques.

## 📄 License

This project is part of Upenn distributed system lab.
