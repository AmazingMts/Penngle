package cis5550.test;

import cis5550.flame.*;
import cis5550.jobs.Crawler;
import cis5550.kvs.*;
import cis5550.tools.Hasher;
import cis5550.tools.Logger;

import java.lang.management.ManagementFactory;
import java.lang.management.OperatingSystemMXBean;
import java.text.DecimalFormat;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.io.*;

public class JobsBenchmark {
    private static final Logger logger = Logger.getLogger(JobsBenchmark.class);
    private static final DecimalFormat df = new DecimalFormat("#.##");

    static class SystemMonitor {
        private final Runtime runtime;
        private final OperatingSystemMXBean osBean;
        private long lastCpuTime = 0;
        private long lastUptime = 0;

        // Store performance data points
        private static List<SystemMonitor.SystemMetrics> metricsHistory;

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


    static class CrawlerBenchmarkConfig {
        static final int MONITORING_INTERVAL = 5; // 5 seconds
        static final String[] TEST_METRICS = {
                "Pages Crawled",
                "Crawl Rate (pages/sec)",
                "Success Rate",
                "Memory Usage (MB)",
                "CPU Usage (%)"
        };
    }

    private static void runCrawlerBenchmark(FlameContext ctx, KVSClient kvs, String seedUrl, SystemMonitor monitor) throws Exception {
        System.out.println("\n=== Starting Crawler Benchmark ===");

        // Initialize metrics
        AtomicLong pagesCrawled = new AtomicLong(0);
        long startTime = System.currentTimeMillis();

        // Create monitoring thread
        Thread monitoringThread = new Thread(() -> {
            while (!Thread.interrupted()) {
                try {
                    updateCrawlerMetrics(kvs, pagesCrawled, startTime, monitor);
                    Thread.sleep(CrawlerBenchmarkConfig.MONITORING_INTERVAL * 1000);
                } catch (InterruptedException e) {
                    break;
                }
            }
        });

        // Start monitoring
        monitoringThread.start();

        try {
            // Setup crawl tables
            kvs.persist("pt-crawl");
            kvs.delete("pt-crawl");

            // Start crawler
            long crawlStartTime = System.currentTimeMillis();
            Crawler.run(ctx, new String[]{seedUrl});
            long crawlEndTime = System.currentTimeMillis();

            // Wait a moment for any remaining operations to complete
            Thread.sleep(1000);

            long finalPageCount = 0;
            long finalSuccessCount = 0;
            Set<String> uniqueUrls = new HashSet<>();

            Iterator<Row> finalPages = kvs.scan("pt-crawl");
            while (finalPages.hasNext()) {
                Row page = finalPages.next();
                String url = page.get("url");
                if (!uniqueUrls.contains(url)) {
                    uniqueUrls.add(url);
                    finalPageCount++;
                    if ("200".equals(page.get("responseCode"))) {
                        finalSuccessCount++;
                    }
                }
            }

            double totalTimeSeconds = (crawlEndTime - crawlStartTime) / 1000.0;
            double finalCrawlRate = finalPageCount / totalTimeSeconds;
            double finalSuccessRate = (finalSuccessCount * 100.0) / finalPageCount;

            String[] headers = CrawlerBenchmarkConfig.TEST_METRICS;
            List<String[]> rows = new ArrayList<>();
            rows.add(new String[]{
                    String.valueOf(finalPageCount),
                    df.format(finalCrawlRate),
                    df.format(finalSuccessRate) + "%",
                    df.format(Runtime.getRuntime().totalMemory() / (1024 * 1024)),
                    df.format(monitor.collectMetrics().cpuUsage)
            });

            printResultTable("Crawler Benchmark Results", headers, rows);

        } finally {
            monitoringThread.interrupt();
            monitoringThread.join();
        }
    }

    private static void updateCrawlerMetrics(KVSClient kvs,
                                             AtomicLong pagesCrawled,
                                             long startTime, SystemMonitor monitor) {
        try {
            Iterator<Row> currentPages = kvs.scan("pt-crawl");
            long currentCount = 0;
            Set<String> currentUrls = new HashSet<>();

            while (currentPages.hasNext()) {
                Row page = currentPages.next();
                String url = page.get("url");
                if (url != null) {
                    currentUrls.add(url);
                    currentCount++;
                }
            }

            pagesCrawled.set(currentUrls.size());

            long currentTime = System.currentTimeMillis();
            double elapsedSeconds = (currentTime - startTime) / 1000.0;

            if (elapsedSeconds > 0) {
                double crawlRate = pagesCrawled.get() / elapsedSeconds;

                System.out.printf("\nCrawler Metrics (%.1f seconds elapsed):%n", elapsedSeconds);
                System.out.printf("Pages Crawled: %d%n", pagesCrawled.get());
                System.out.printf("Crawl Rate: %.2f pages/sec%n", crawlRate);

//                monitor.collectAndPrintMetrics("Crawler Test");
            }
        } catch (Exception e) {
            logger.error("Error updating metrics", e);
        }
    }

    static class PageRankBenchmarkConfig {
        static final String[] TEST_METRICS = {
                "Total Pages",
                "Iterations",
                "Convergence Time (sec)",
                "Memory Usage (MB)",
                "CPU Usage (%)",
                "Max PageRank Value",
                "Min PageRank Value"
        };
    }

    private static void runPageRankBenchmark(FlameContext ctx, KVSClient kvs, SystemMonitor monitor) throws Exception {
        System.out.println("\n=== Starting PageRank Benchmark ===");

        long startTime = System.currentTimeMillis();
        AtomicLong currentIteration = new AtomicLong(0);
        Map<String, Double> lastPRValues = new ConcurrentHashMap<>();

        Thread monitoringThread = new Thread(() -> {
            while (!Thread.interrupted()) {
                try {
                    try {
                        int totalPages = 0;
                        double maxPR = -1;
                        double minPR = -1;
                        boolean hasChanges = false;

                        Iterator<Row> prRows = kvs.scan("pt-pageranks");
                        Map<String, Double> currentValues = new HashMap<>();

                        while (prRows.hasNext()) {
                            Row row = prRows.next();
                            String url = row.key();  // 使用key而不是url列
                            String prValue = row.get("pagerank");

                            if (prValue != null) {
                                totalPages++;
                                try {
                                    double pr = Double.parseDouble(prValue);
                                    currentValues.put(url, pr);

                                    if (maxPR == -1 || pr > maxPR) {
                                        maxPR = pr;
                                    }
                                    if (minPR == -1 || pr < minPR) {
                                        minPR = pr;
                                    }

                                    Double lastValue = lastPRValues.get(url);
                                    if (lastValue == null || Math.abs(lastValue - pr) > 0.0001) {
                                        hasChanges = true;
                                    }
                                } catch (NumberFormatException e) {
                                    logger.error("Invalid pagerank value: " + prValue);
                                }
                            }
                        }

                        if (hasChanges && !currentValues.isEmpty()) {
                            lastPRValues.clear();
                            lastPRValues.putAll(currentValues);
                            currentIteration.incrementAndGet();
                        }

                        long currentTime = System.currentTimeMillis();
                        double elapsedSeconds = (currentTime - startTime) / 1000.0;

//                        System.out.printf("\nPageRank Metrics (%.1f seconds elapsed):%n", elapsedSeconds);
//                        System.out.printf("Total Pages: %d%n", totalPages);
//                        System.out.printf("Current Iteration: %d%n", currentIteration.get());
//
//                        if (maxPR != -1 && minPR != -1) {
//                            System.out.printf("Current Max PR: %.6f%n", maxPR);
//                            System.out.printf("Current Min PR: %.6f%n", minPR);
//                        } else {
//                            System.out.println("No PageRank values available yet");
//                        }

                        monitor.collectAndPrintMetrics("PageRank Test");

                    } catch (Exception e) {
                        logger.error("Error updating metrics", e);
                    }

                    Thread.sleep(CrawlerBenchmarkConfig.MONITORING_INTERVAL * 1000);
                } catch (InterruptedException e) {
                    break;
                }
            }
        });

        monitoringThread.start();

        try {
            kvs.delete("pt-pageranks");

            long prStartTime = System.currentTimeMillis();
            cis5550.jobs.PageRank.run(ctx, new String[]{"0.01", "99"});
            long prEndTime = System.currentTimeMillis();

            double maxPR = 0.0;
            double minPR = Double.MAX_VALUE;
            int pageCount = 0;
            Iterator<Row> prResults = kvs.scan("pt-pageranks");
            while (prResults.hasNext()) {
                Row row = prResults.next();
                String prValue = row.get("pagerank");
                if (prValue != null) {
                    double pr = Double.parseDouble(prValue);
                    maxPR = Math.max(maxPR, pr);
                    minPR = Math.min(minPR, pr);
                    pageCount++;
                }
            }

            double totalTimeSeconds = (prEndTime - prStartTime) / 1000.0;

            String[] headers = PageRankBenchmarkConfig.TEST_METRICS;
            List<String[]> rows = new ArrayList<>();
            rows.add(new String[]{
                    String.valueOf(pageCount),
                    String.valueOf(currentIteration.get()),
                    df.format(totalTimeSeconds),
                    df.format(Runtime.getRuntime().totalMemory() / (1024 * 1024)),
                    df.format(monitor.collectMetrics().cpuUsage),
                    df.format(maxPR),
                    df.format(minPR)
            });

            printResultTable("PageRank Benchmark Results", headers, rows);

            System.out.println("\nPageRank Distribution:");
            Map<Double, Integer> distribution = new TreeMap<>();
            prResults = kvs.scan("pt-pageranks");
            while (prResults.hasNext()) {
                Row row = prResults.next();
                String prValue = row.get("pagerank");
                if (prValue != null) {
                    double pr = Double.parseDouble(prValue);
                    double bucket = Math.floor(pr * 10) / 10.0;
                    distribution.put(bucket, distribution.getOrDefault(bucket, 0) + 1);
                }
            }

            for (Map.Entry<Double, Integer> entry : distribution.entrySet()) {
                System.out.printf("PageRank %.1f: %d pages%n", entry.getKey(), entry.getValue());
            }

        } finally {
            monitoringThread.interrupt();
            monitoringThread.join();
        }
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

    static class URLGenerator {
        private static final String[] DOMAINS = {"example.com", "testsite.net", "random.org", "mysite.io"};
        private static final Random RANDOM = new Random();

        public static String generateRandomURL() {
            String domain = DOMAINS[RANDOM.nextInt(DOMAINS.length)];
            String path = "/page" + RANDOM.nextInt(1000);
            return "http://" + domain + path;
        }
    }

    static class HTMLPageGenerator {
        private static final Random RANDOM = new Random();

        public static String generateRandomHTMLPage(int maxNumberOfURLsPerPage) {
            StringBuilder html = new StringBuilder();
            html.append("<!DOCTYPE html>\n<html>\n<head>\n");
            html.append("<title>Random Page</title>\n");
            html.append("</head>\n<body>\n");
            html.append("<h1>Welcome to the Random Page</h1>\n");
            html.append("<p>This page contains randomly generated links:</p>\n");

            int numURL = RANDOM.nextInt(maxNumberOfURLsPerPage);
            for (int i = 0; i < numURL; i++) {
                String url = URLGenerator.generateRandomURL();
                html.append("<a href=\"").append(url).append("\">Link ").append(i + 1).append("</a><br>\n");
            }

            html.append("</body>\n</html>");
            return html.toString();
        }
    }

    private static void generateTestPTCrawl(KVSClient kvs, int numOfURLs, int maxNumOdURLsPerPage) throws IOException {
        System.out.println("\n=== Generating Random Crawl Data with " + numOfURLs + " URLs ===");
        kvs.delete("pt-crawl");
        kvs.persist("pt-crawl");

        for (int i = 0; i < numOfURLs; i++) {
            String url = URLGenerator.generateRandomURL();
            String page = HTMLPageGenerator.generateRandomHTMLPage(maxNumOdURLsPerPage);
            String responseCode = "200";
            String urlHash = Hasher.hash(url);

            Row row = new Row(urlHash);
            row.put("url", url);
            row.put("rawHTML", page);
            row.put("responseCode", responseCode);
            kvs.putRow("pt-crawl", row);
        }
//        System.out.println("");
    }

    public static void run(FlameContext ctx, String[] args) throws Exception {
        try {
            KVSClient kvs = ctx.getKVS();
            SystemMonitor systemMonitor = new SystemMonitor();
            System.out.println("\n=== Starting Jobs Benchmark Suite ===");
            System.out.printf("KVS Workers: %d, Flame Workers: %d\n\n",
                    kvs.numWorkers(), ctx.getWorkerCount());

            String seedUrl = "http://advanced.crawltest.401.cis5550.net";
            if (args != null && args.length > 0) {
                if ("crawl".equals(args[0])) {
                    runCrawlerBenchmark(ctx, kvs, seedUrl, systemMonitor);
        //                    systemMonitor.collectAndPrintMetrics("Crawler Test");
                    SystemMonitor.printSummary();

                    runPageRankBenchmark(ctx, kvs, systemMonitor);
                    SystemMonitor.printSummary();
                } else if ("pagerank".equals(args[0])) {
                    System.out.println("Running PageRank Benchmark");
                    int numOfURLs = 1000;
                    int maxNumOfURLsPerPage = 10;
                    if (args.length > 1) {
                        numOfURLs = Integer.parseInt(args[1]);
                    }
                    if (args.length > 2) {
                        maxNumOfURLsPerPage = Integer.parseInt(args[2]);
                    }
                    generateTestPTCrawl(kvs, numOfURLs, maxNumOfURLsPerPage);
                    runPageRankBenchmark(ctx, kvs, systemMonitor);
                    SystemMonitor.printSummary();
                }
            }

            // End benchmark
            try {
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
        JobsBenchmark.run(null, args);
    }
}
