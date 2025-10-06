package cis5550.test;

import cis5550.flame.*;
import cis5550.jobs.Crawler;
import cis5550.kvs.*;
import cis5550.tools.Logger;

import java.lang.management.ManagementFactory;
import java.lang.management.OperatingSystemMXBean;
import java.text.DecimalFormat;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.io.*;

public class KvsBenchmark {
    private static final Logger logger = Logger.getLogger(KvsBenchmark.class);
    private static final DecimalFormat df = new DecimalFormat("#.##");
    private static final int DEFAULT_BATCH_SIZE = 1000;

    static class BenchmarkConfig {
        static final int NUM_OPERATIONS = 1000;
        static final int DATA_SIZE = 1024;
        static final int CONCURRENT_THREADS = 10;
        static final int CONCURRENT_DURATION = 60;
        static final double READ_WRITE_RATIO = 0.8;
        static final int STRESS_DURATION = 300;
        static final int STRESS_INITIAL_THREADS = 5;
        static final int STRESS_THREAD_INCREMENT = 5;
        static final int STRESS_MAX_THREADS = 50;
        static final int[] BATCH_OPERATIONS = {1000, 10000, 100000};
        static final int[] BATCH_SIZES = {1024, 5120, 10240};
    }

    static class OperationMetrics {
        long totalOperations;
        long successfulOperations;
        long totalLatency;
        long maxLatency;
        double avgLatency;
        double tps;

        OperationMetrics() {
            this.totalOperations = 0;
            this.successfulOperations = 0;
            this.totalLatency = 0;
            this.maxLatency = 0;
            this.avgLatency = 0.0;
            this.tps = 0.0;
        }

        void recordOperation(long latency, boolean success) {
            totalOperations++;
            if (success) {
                successfulOperations++;
                totalLatency += latency;
                maxLatency = Math.max(maxLatency, latency);
                avgLatency = totalLatency / (double) successfulOperations;
            }
        }

        void calculateTPS(long duration) {
            if (duration <= 0) {
                tps = 0.0;
                return;
            }

            tps = (successfulOperations * 1000.0) / duration;
        }
    }

    static class ResourceMetrics {
        private final Runtime runtime = Runtime.getRuntime();
        private final OperatingSystemMXBean osBean = ManagementFactory.getOperatingSystemMXBean();
        double cpuLoad;
        long usedMemory;
        long totalMemory;

        void update() {
            cpuLoad = osBean.getSystemLoadAverage();
            usedMemory = (runtime.totalMemory() - runtime.freeMemory()) / (1024 * 1024);
            totalMemory = runtime.totalMemory() / (1024 * 1024);
        }

        String[] getMetricsRow() {
            return new String[]{
                    df.format(cpuLoad * 100) + "%",
                    usedMemory + "MB",
                    totalMemory + "MB"
            };
        }
    }

    static class SystemMonitor {
        private final Runtime runtime;
        private final OperatingSystemMXBean osBean;
        private long lastCpuTime = 0;
        private long lastUptime = 0;

        // Store performance data points
        private static List<SystemMetrics> metricsHistory;

        static class SystemMetrics {
            long timestamp;
            double cpuUsage;
            long usedMemory;
            long totalMemory;
            double systemLoad;
            int threadCount;

            SystemMetrics() {
                this.timestamp = System.currentTimeMillis();
            }

            String[] toRow() {
                return new String[]{
                        new Date(timestamp).toString(),
                        df.format(cpuUsage) + "%",
                        (usedMemory / 1024 / 1024) + "MB",
                        (totalMemory / 1024 / 1024) + "MB",
                        df.format(systemLoad),
                        String.valueOf(threadCount)
                };
            }
        }

        SystemMonitor() {
            this.runtime = Runtime.getRuntime();
            this.osBean = ManagementFactory.getOperatingSystemMXBean();
            this.metricsHistory = new ArrayList<>();
        }

        void startMonitoring() {
            // Reset history
            metricsHistory = new ArrayList<>();
            scheduleMetricsCollection();
        }

        private void scheduleMetricsCollection() {
            ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
            scheduler.scheduleAtFixedRate(() -> {
                SystemMetrics metrics = collectMetrics();
                metricsHistory.add(metrics);
                printCurrentMetrics(metrics);
            }, 0, 5, TimeUnit.SECONDS);
        }

        public void collectAndPrintMetrics(String testName) {
            SystemMetrics metrics = collectMetrics();
            metricsHistory.add(metrics);
            printCurrentMetrics(metrics, testName);
        }

        private void printCurrentMetrics(SystemMetrics metrics, String testName) {
            System.out.println("\n=== System Metrics for " + testName + " ===");
            String[] headers = {"Metric", "Value"};
            List<String[]> rows = new ArrayList<>();

            rows.add(new String[]{"CPU Usage", df.format(metrics.cpuUsage) + "%"});
            rows.add(new String[]{"Memory Usage",
                    (metrics.usedMemory / 1024 / 1024) + "MB / " +
                            (metrics.totalMemory / 1024 / 1024) + "MB"});
            rows.add(new String[]{"System Load", df.format(metrics.systemLoad)});
            rows.add(new String[]{"Thread Count", String.valueOf(metrics.threadCount)});

            printResultTable("System Metrics", headers, rows);
        }

        private SystemMetrics collectMetrics() {
            SystemMetrics metrics = new SystemMetrics();

            // getSystemLoadAverage() returns a value between 0.0 and 1.0
            metrics.cpuUsage = Math.min(osBean.getSystemLoadAverage() * 100, 100.0);

            // Alternative CPU usage calculation
            if (osBean instanceof com.sun.management.OperatingSystemMXBean) {
                com.sun.management.OperatingSystemMXBean sunOsBean =
                        (com.sun.management.OperatingSystemMXBean) osBean;
                metrics.cpuUsage = sunOsBean.getProcessCpuLoad() * 100;
            }

            metrics.usedMemory = runtime.totalMemory() - runtime.freeMemory();
            metrics.totalMemory = runtime.totalMemory();
            metrics.systemLoad = osBean.getSystemLoadAverage();
            metrics.threadCount = Thread.activeCount();
            return metrics;
        }

        private void printCurrentMetrics(SystemMetrics metrics) {
            System.out.println("\n=== System Metrics ===");
            String[] headers = {"Metric", "Value"};
            List<String[]> rows = new ArrayList<>();

            rows.add(new String[]{"CPU Usage", df.format(metrics.cpuUsage) + "%"});
            rows.add(new String[]{"Memory Usage",
                    (metrics.usedMemory / 1024 / 1024) + "MB / " +
                            (metrics.totalMemory / 1024 / 1024) + "MB"});
            rows.add(new String[]{"System Load", df.format(metrics.systemLoad)});
            rows.add(new String[]{"Thread Count", String.valueOf(metrics.threadCount)});

            printResultTable("Current System Performance", headers, rows);
        }

        static void printSummary() {
            if (metricsHistory.isEmpty()) return;

            System.out.println("\n=== System Performance Summary ===");

            // Calculate averages
            double avgCpuUsage = metricsHistory.stream()
                    .mapToDouble(m -> m.cpuUsage)
                    .average()
                    .orElse(0.0);

            double avgMemoryUsage = metricsHistory.stream()
                    .mapToDouble(m -> m.usedMemory)
                    .average()
                    .orElse(0.0);

            double avgSystemLoad = metricsHistory.stream()
                    .mapToDouble(m -> m.systemLoad)
                    .average()
                    .orElse(0.0);

            double avgThreadCount = metricsHistory.stream()
                    .mapToDouble(m -> m.threadCount)
                    .average()
                    .orElse(0.0);

            // Print summary table
            String[] headers = {"Metric", "Average", "Peak"};
            List<String[]> rows = new ArrayList<>();

            rows.add(new String[]{
                    "CPU Usage",
                    df.format(avgCpuUsage) + "%",
                    df.format(metricsHistory.stream()
                            .mapToDouble(m -> m.cpuUsage)
                            .max()
                            .orElse(0.0)) + "%"
            });

            rows.add(new String[]{
                    "Memory Usage (MB)",
                    df.format(avgMemoryUsage / 1024 / 1024),
                    df.format(metricsHistory.stream()
                            .mapToDouble(m -> m.usedMemory)
                            .max()
                            .orElse(0.0) / 1024 / 1024)
            });

            rows.add(new String[]{
                    "System Load",
                    df.format(avgSystemLoad),
                    df.format(metricsHistory.stream()
                            .mapToDouble(m -> m.systemLoad)
                            .max()
                            .orElse(0.0))
            });

            rows.add(new String[]{
                    "Thread Count",
                    df.format(avgThreadCount),
                    df.format(metricsHistory.stream()
                            .mapToDouble(m -> m.threadCount)
                            .max()
                            .orElse(0.0))
            });

            printResultTable("System Performance Summary", headers, rows);
        }
    }

    private static void runBasicTests(KVSClient kvs, String tableName) throws Exception {
        System.out.println("\n=== Starting Basic Tests ===");
        OperationMetrics putMetrics = new OperationMetrics();
        OperationMetrics getMetrics = new OperationMetrics();

        long startPutTime = System.currentTimeMillis();
        // Run PUT operations
        for (int i = 0; i < BenchmarkConfig.NUM_OPERATIONS; i++) {
            String key = "key-" + i;
            String value = generateValue(BenchmarkConfig.DATA_SIZE);

            long startTime = System.nanoTime();
            try {
                kvs.put(tableName, key, "value", value);
                putMetrics.recordOperation(System.nanoTime() - startTime, true);
            } catch (Exception e) {
                putMetrics.recordOperation(0, false);
                logger.error("PUT operation failed", e);
            }
        }
        long putDuration = System.currentTimeMillis() - startPutTime;
        putMetrics.calculateTPS(putDuration);

        long startGetTime = System.currentTimeMillis();
        // Run GET operations
        for (int i = 0; i < BenchmarkConfig.NUM_OPERATIONS; i++) {
            String key = "key-" + i;

            long startTime = System.nanoTime();
            try {
                kvs.get(tableName, key, "value");
                getMetrics.recordOperation(System.nanoTime() - startTime, true);
            } catch (Exception e) {
                getMetrics.recordOperation(0, false);
                logger.error("GET operation failed", e);
            }
        }
        long getDuration = System.currentTimeMillis() - startGetTime;
        getMetrics.calculateTPS(getDuration);

        // Print results
        printBasicTestResults(putMetrics, getMetrics);
    }

    private static void runConcurrentTests(KVSClient kvs, String tableName) throws Exception {
        System.out.println("\n=== Starting Concurrent Tests ===");
        CountDownLatch latch = new CountDownLatch(BenchmarkConfig.CONCURRENT_THREADS);
        ConcurrentHashMap<Integer, OperationMetrics> threadMetrics = new ConcurrentHashMap<>();

        ExecutorService executor = Executors.newFixedThreadPool(BenchmarkConfig.CONCURRENT_THREADS);
        AtomicBoolean shouldStop = new AtomicBoolean(false);

        long startTestTime = System.currentTimeMillis();

        for (int i = 0; i < BenchmarkConfig.CONCURRENT_THREADS; i++) {
            final int threadId = i;
            executor.submit(() -> {
                OperationMetrics metrics = new OperationMetrics();
                threadMetrics.put(threadId, metrics);

                try {
                    while (!shouldStop.get()) {
                        if (Math.random() < BenchmarkConfig.READ_WRITE_RATIO) {
                            // Perform READ
                            String key = "key-" + ThreadLocalRandom.current().nextInt(BenchmarkConfig.NUM_OPERATIONS);
                            long startTime = System.nanoTime();
                            try {
                                kvs.get(tableName, key, "value");
                                metrics.recordOperation(System.nanoTime() - startTime, true);
                            } catch (Exception e) {
                                metrics.recordOperation(0, false);
                            }
                        } else {
                            // Perform WRITE
                            String key = "key-" + ThreadLocalRandom.current().nextInt(BenchmarkConfig.NUM_OPERATIONS);
                            String value = generateValue(BenchmarkConfig.DATA_SIZE);
                            long startTime = System.nanoTime();
                            try {
                                kvs.put(tableName, key, "value", value);
                                metrics.recordOperation(System.nanoTime() - startTime, true);
                            } catch (Exception e) {
                                metrics.recordOperation(0, false);
                            }
                        }
                    }
                } finally {
                    latch.countDown();
                }
            });
        }

        // Run for specified duration
        Thread.sleep(BenchmarkConfig.CONCURRENT_DURATION * 1000);
        shouldStop.set(true);
        latch.await(5, TimeUnit.SECONDS);
        executor.shutdownNow();

        long duration = System.currentTimeMillis() - startTestTime;
        threadMetrics.forEach((threadId, metrics) -> {metrics.calculateTPS(duration);});

        // Print results
        printConcurrentTestResults(threadMetrics);
    }

    private static void runStressTests(KVSClient kvs, String tableName) throws Exception {
        System.out.println("\n=== Starting Stress Tests ===");
        List<OperationMetrics> stressResults = new ArrayList<>();

        for (int threadCount = BenchmarkConfig.STRESS_INITIAL_THREADS;
             threadCount <= BenchmarkConfig.STRESS_MAX_THREADS;
             threadCount += BenchmarkConfig.STRESS_THREAD_INCREMENT) {

            System.out.printf("\nRunning stress test with %d threads\n", threadCount);
            OperationMetrics metrics = runStressIteration(kvs, tableName, threadCount);
            stressResults.add(metrics);
        }

        // Print results
        printStressTestResults(stressResults);
    }

    private static OperationMetrics runStressIteration(KVSClient kvs, String tableName, int threadCount) throws Exception {
        OperationMetrics metrics = new OperationMetrics();
        CountDownLatch latch = new CountDownLatch(threadCount);
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        AtomicBoolean shouldStop = new AtomicBoolean(false);

        long startTestTime = System.currentTimeMillis();
        for (int i = 0; i < threadCount; i++) {
            executor.submit(() -> {
                try {
                    while (!shouldStop.get()) {
                        String key = "key-" + ThreadLocalRandom.current().nextInt(1000000);
                        String value = generateValue(BenchmarkConfig.DATA_SIZE);

                        long startTime = System.nanoTime();
                        try {
                            kvs.put(tableName, key, "value", value);
                            metrics.recordOperation(System.nanoTime() - startTime, true);
                        } catch (Exception e) {
                            metrics.recordOperation(0, false);
                        }
                    }
                } finally {
                    latch.countDown();
                }
            });
        }

        Thread.sleep(BenchmarkConfig.STRESS_DURATION * 1000 / 10); // Run each iteration for 1/10th of total duration
        shouldStop.set(true);
        latch.await(5, TimeUnit.SECONDS);
        executor.shutdownNow();

        long duration = System.currentTimeMillis() - startTestTime;

        metrics.calculateTPS(duration);
        return metrics;
    }

    private static void runBatchTests(KVSClient kvs, String tableName) throws Exception {
        System.out.println("\n=== Starting Batch Tests ===");
        List<String[]> results = new ArrayList<>();

        // Test different batch sizes and operation counts
        for (int numOperations : BenchmarkConfig.BATCH_OPERATIONS) {
            for (int dataSize : BenchmarkConfig.BATCH_SIZES) {
                OperationMetrics metrics = new OperationMetrics();
                int batchesCompleted = 0;

                System.out.printf("\nTesting with %d operations, %d bytes per operation\n",
                        numOperations, dataSize);

                long startTime = System.nanoTime();
                long startTestTime = System.currentTimeMillis();
                int currentBatch = 0;

                // Perform batch operations
                for (int i = 0; i < numOperations; i++) {
                    String key = "batch-" + i;
                    String value = generateValue(dataSize);

                    try {
                        kvs.bufferedPut(tableName, key, "value", value);
                        currentBatch++;

                        // Flush when batch size is reached
                        if (currentBatch >= DEFAULT_BATCH_SIZE) {
                            long batchStartTime = System.nanoTime();
                            kvs.flushPutBuffer();
                            metrics.recordOperation(System.nanoTime() - batchStartTime, true);
                            batchesCompleted++;
                            currentBatch = 0;
                        }
                    } catch (Exception e) {
                        metrics.recordOperation(0, false);
                        logger.error("Batch operation failed", e);
                    }
                }

                // Flush remaining operations
                if (currentBatch > 0) {
                    try {
                        long batchStartTime = System.nanoTime();
                        kvs.flushPutBuffer();
                        metrics.recordOperation(System.nanoTime() - batchStartTime, true);
                        batchesCompleted++;
                    } catch (Exception e) {
                        metrics.recordOperation(0, false);
                    }
                }

                long totalTime = System.nanoTime() - startTime;
                double operationsPerSecond = numOperations / (totalTime / 1_000_000_000.0);

                long duration = System.currentTimeMillis() - startTestTime;
                metrics.calculateTPS(duration);

                results.add(new String[]{
                        String.valueOf(numOperations),
                        String.valueOf(dataSize),
                        String.valueOf(batchesCompleted),
                        df.format(metrics.avgLatency / 1_000_000.0),  // Convert to ms
                        df.format(metrics.maxLatency / 1_000_000.0),
                        df.format(operationsPerSecond),
                        df.format(metrics.successfulOperations * 100.0 / metrics.totalOperations) + "%",
                        df.format(metrics.tps)
                });
            }
        }

        // Print batch test results
        String[] headers = {
                "Operations",
                "Data Size (bytes)",
                "Batches",
                "Avg Batch Latency (ms)",
                "Max Batch Latency (ms)",
                "Operations/s",
                "Success Rate",
                "TPS"
        };

        printResultTable("Batch Test Results", headers, results);
    }

    private static void runCrawlerTest(FlameContext cxt, String seedUrl) throws Exception {

        Crawler.run(cxt, new String[]{seedUrl});

    }

    private static void printBasicTestResults(OperationMetrics putMetrics, OperationMetrics getMetrics) {
        String[] headers = {"Operation", "Total", "Successful", "Avg Latency (ms)", "Max Latency (ms)", "TPS"};
        List<String[]> rows = new ArrayList<>();

        rows.add(new String[]{
                "PUT",
                String.valueOf(putMetrics.totalOperations),
                String.valueOf(putMetrics.successfulOperations),
                df.format(putMetrics.avgLatency / 1_000_000.0),
                df.format(putMetrics.maxLatency / 1_000_000.0),
                df.format(putMetrics.tps)
        });

        rows.add(new String[]{
                "GET",
                String.valueOf(getMetrics.totalOperations),
                String.valueOf(getMetrics.successfulOperations),
                df.format(getMetrics.avgLatency / 1_000_000.0),
                df.format(getMetrics.maxLatency / 1_000_000.0),
                df.format(getMetrics.tps)
        });

        printResultTable("Basic Test Results", headers, rows);
    }

    private static void printConcurrentTestResults(ConcurrentHashMap<Integer, OperationMetrics> threadMetrics) {
        String[] headers = {"Thread", "Operations", "Success Rate", "Avg Latency (ms)", "Max Latency (ms)", "TPS"};
        List<String[]> rows = new ArrayList<>();

        threadMetrics.forEach((threadId, metrics) -> {
            rows.add(new String[]{
                    String.valueOf(threadId),
                    String.valueOf(metrics.totalOperations),
                    df.format(metrics.successfulOperations * 100.0 / metrics.totalOperations) + "%",
                    df.format(metrics.avgLatency / 1_000_000.0),
                    df.format(metrics.maxLatency / 1_000_000.0),
                    df.format(metrics.tps)
            });
        });

        printResultTable("Concurrent Test Results", headers, rows);
    }

    private static void printStressTestResults(List<OperationMetrics> results) {
        String[] headers = {"Threads", "Operations/s", "Success Rate", "Avg Latency (ms)", "Max Latency (ms)", "TPS"};
        List<String[]> rows = new ArrayList<>();

        int threadCount = BenchmarkConfig.STRESS_INITIAL_THREADS;
        for (OperationMetrics metrics : results) {
            rows.add(new String[]{
                    String.valueOf(threadCount),
                    df.format(metrics.tps),
                    df.format(metrics.successfulOperations * 100.0 / metrics.totalOperations) + "%",
                    df.format(metrics.avgLatency / 1_000_000.0),
                    df.format(metrics.maxLatency / 1_000_000.0),
                    df.format(metrics.tps)
            });
            threadCount += BenchmarkConfig.STRESS_THREAD_INCREMENT;
        }

        printResultTable("Stress Test Results", headers, rows);
    }

    private static void printResultTable(String title, String[] headers, List<String[]> rows) {
        // Calculate column widths
        int[] columnWidths = new int[headers.length];
        for (int i = 0; i < headers.length; i++) {
            columnWidths[i] = headers[i].length();
            for (String[] row : rows) {
                columnWidths[i] = Math.max(columnWidths[i], row[i].length());
            }
            columnWidths[i] += 2; // Add padding
        }

        // Print header
        System.out.println("\n=== " + title + " ===");
        printSeparator(columnWidths);
        printRow(headers, columnWidths);
        printSeparator(columnWidths);

        // Print rows
        for (String[] row : rows) {
            printRow(row, columnWidths);
        }
        printSeparator(columnWidths);
    }

    private static void printSeparator(int[] widths) {
        StringBuilder sb = new StringBuilder("+");
        for (int width : widths) {
            for (int i = 0; i < width; i++) {
                sb.append("-");
            }
            sb.append("+");
        }
        System.out.println(sb);
    }

    private static void printRow(String[] row, int[] widths) {
        StringBuilder sb = new StringBuilder("|");
        for (int i = 0; i < row.length; i++) {
            String cell = row[i];
            int padding = widths[i] - cell.length();
            sb.append(" ").append(cell);
            for (int j = 0; j < padding - 1; j++) {
                sb.append(" ");
            }
            sb.append("|");
        }
        System.out.println(sb);
    }

    private static String generateValue(int size) {
        StringBuilder sb = new StringBuilder(size);
        for (int i = 0; i < size; i++) {
            sb.append((char) ('a' + (i % 26)));
        }
        return sb.toString();
    }

    public static void run(FlameContext ctx, String[] args) throws Exception {
        try {
            String testTable = "pt-benchmark-test-" + System.currentTimeMillis();
            KVSClient kvs = ctx.getKVS();
            kvs.persist(testTable);
            if (args != null && args.length > 0) {
                if ("normal".equals(args[0])) {
                    testTable = "benchmark-test-" + System.currentTimeMillis();
                } else if ("persist".equals(args[0])) {
                    testTable = "benchmark-test-" + System.currentTimeMillis();
                    kvs.persist(testTable);
                }
            }

            SystemMonitor systemMonitor = new SystemMonitor();

            System.out.println("\n=== Starting KVS Benchmark Suite ===");
            System.out.printf("KVS Workers: %d, Flame Workers: %d\n\n",
                    kvs.numWorkers(), ctx.getWorkerCount());


            if (args != null && args.length > 1 && "1".equals(args[1])) {
                // run tests with system monitoring every 5 seconds
                systemMonitor.startMonitoring();
                runBasicTests(kvs, testTable);
                runConcurrentTests(kvs, testTable);
                runStressTests(kvs, testTable);
                runBatchTests(kvs, testTable);
            } else {
                // run tests and system monitoring after each test
                runBasicTests(kvs, testTable);
                systemMonitor.collectAndPrintMetrics("Basic Tests");

                runConcurrentTests(kvs, testTable);
                systemMonitor.collectAndPrintMetrics("Concurrent Tests");

                runStressTests(kvs, testTable);
                systemMonitor.collectAndPrintMetrics("Stress Tests");

                runBatchTests(kvs, testTable);
                systemMonitor.collectAndPrintMetrics("Batch Tests");
            }

            SystemMonitor.printSummary();

            // Clean up
            try {
                if (testTable.startsWith("benchmark-test-")) {
                    kvs.delete(testTable);
                }
                ctx.output("OK");
            } catch (Exception e) {
                logger.error("Failed to clean up test table", e);
            }

        } catch (Exception e) {
            logger.error("Benchmark failed", e);
            throw e;
        }
    }

    public static void main(String[] args) throws Exception {
        run(null, args);
    }
}