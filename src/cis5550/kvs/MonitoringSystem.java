package cis5550.kvs;

import java.util.concurrent.*;
import java.util.*;
import cis5550.tools.Logger;

public class MonitoringSystem {
    private static final Logger logger = Logger.getLogger(MonitoringSystem.class);

    final Map<String, WorkerMetrics> workerMetrics;
    private final Map<String, TableMetrics> tableMetrics;
    private final ScheduledExecutorService scheduler;

    public static class WorkerMetrics {
        public long requestCount;
        public long errorCount;
        public long bytesTransferred;
        public long responseTimeTotal;
        public long lastHeartbeat;
        public double cpuUsage;
        public long memoryUsage;

        public synchronized void recordRequest(long bytes, long responseTime, boolean error) {
            requestCount++;
            bytesTransferred += bytes;
            responseTimeTotal += responseTime;
            if (error) errorCount++;
        }
    }

    public static class TableMetrics {
        public long rowCount;
        public long totalSize;
        public long readCount;
        public long writeCount;
        public Map<String, Long> columnStats = new ConcurrentHashMap<>();

        public synchronized void recordOperation(boolean isWrite, long bytes) {
            if (isWrite) writeCount++;
            else readCount++;
            totalSize += bytes;
        }
    }

    public MonitoringSystem() {
        this.workerMetrics = new ConcurrentHashMap<>();
        this.tableMetrics = new ConcurrentHashMap<>();
        this.scheduler = Executors.newScheduledThreadPool(1);

        // Schedule periodic metrics collection
        scheduler.scheduleAtFixedRate(this::collectMetrics, 0, 1, TimeUnit.MINUTES);
    }

    public void recordWorkerHeartbeat(String workerId, double cpu, long memory) {
        WorkerMetrics metrics = workerMetrics.computeIfAbsent(workerId, k -> new WorkerMetrics());
        metrics.lastHeartbeat = System.currentTimeMillis();
        metrics.cpuUsage = cpu;
        metrics.memoryUsage = memory;
    }

    public void recordTableOperation(String tableName, String operation, long bytes) {
        TableMetrics metrics = tableMetrics.computeIfAbsent(tableName, k -> new TableMetrics());
        metrics.recordOperation(operation.equalsIgnoreCase("write"), bytes);
    }

    private void collectMetrics() {
        try {
            // Remove metrics for inactive workers
            long cutoff = System.currentTimeMillis() - TimeUnit.MINUTES.toMillis(5);
            workerMetrics.entrySet().removeIf(e -> e.getValue().lastHeartbeat < cutoff);

            // Generate monitoring report
            StringBuilder report = new StringBuilder();
            report.append("=== KVS Monitoring Report ===\n");

            // Worker statistics
            report.append("\nWorker Statistics:\n");
            workerMetrics.forEach((workerId, metrics) -> {
                report.append(String.format("Worker %s:\n", workerId));
                report.append(String.format("  Requests: %d (Errors: %d)\n",
                        metrics.requestCount, metrics.errorCount));
                report.append(String.format("  Avg Response Time: %.2fms\n",
                        metrics.requestCount > 0 ? metrics.responseTimeTotal / (double)metrics.requestCount : 0));
                report.append(String.format("  CPU Usage: %.1f%%\n", metrics.cpuUsage));
                report.append(String.format("  Memory Usage: %dMB\n", metrics.memoryUsage / (1024 * 1024)));
            });

            // Table statistics
            report.append("\nTable Statistics:\n");
            tableMetrics.forEach((tableName, metrics) -> {
                report.append(String.format("Table %s:\n", tableName));
                report.append(String.format("  Rows: %d\n", metrics.rowCount));
                report.append(String.format("  Size: %dMB\n", metrics.totalSize / (1024 * 1024)));
                report.append(String.format("  Reads/Writes: %d/%d\n",
                        metrics.readCount, metrics.writeCount));
            });

            logger.info(report.toString());

        } catch (Exception e) {
            logger.error("Error collecting metrics", e);
        }
    }

    public String generateHTMLReport() {
        StringBuilder html = new StringBuilder();
        html.append("<html><head><title>KVS Monitoring</title>");
        html.append("<style>");
        html.append("table { border-collapse: collapse; width: 100%; margin-bottom: 20px; }");
        html.append("th, td { border: 1px solid #ddd; padding: 8px; text-align: left; }");
        html.append("th { background-color: #f2f2f2; }");
        html.append("h2 { margin: 20px 0 10px 0; }");
        html.append(".no-data { color: #666; font-style: italic; padding: 10px; }");
        html.append("</style></head><body>");

        // Worker Status
        html.append("<h2>Worker Status</h2>");
        html.append("<table>");
        html.append("<tr><th>Worker ID</th><th>Requests</th><th>Errors</th>" +
                "<th>Avg Response Time</th><th>CPU Usage</th><th>Memory Usage</th></tr>");

        boolean hasWorkerData = false;
        for (Map.Entry<String, WorkerMetrics> entry : workerMetrics.entrySet()) {
            hasWorkerData = true;
            WorkerMetrics metrics = entry.getValue();
            double avgResponseTime = metrics.requestCount > 0 ?
                    metrics.responseTimeTotal / (double)metrics.requestCount : 0;

            html.append(String.format("<tr><td>%s</td><td>%d</td><td>%d</td><td>%.2fms</td><td>%.1f%%</td><td>%dMB</td></tr>",
                    entry.getKey(),
                    metrics.requestCount,
                    metrics.errorCount,
                    avgResponseTime,
                    metrics.cpuUsage,
                    metrics.memoryUsage / (1024 * 1024)
            ));
        }

        if (!hasWorkerData) {
            html.append("<tr><td colspan='6' class='no-data'>No worker data available</td></tr>");
        }
        html.append("</table>");

        // Table Statistics
        html.append("<h2>Table Statistics</h2>");
        html.append("<table>");
        html.append("<tr><th>Table Name</th><th>Rows</th><th>Size</th>" +
                "<th>Reads</th><th>Writes</th></tr>");

        boolean hasTableData = false;
        for (Map.Entry<String, TableMetrics> entry : tableMetrics.entrySet()) {
            hasTableData = true;
            TableMetrics metrics = entry.getValue();
            html.append(String.format("<tr><td>%s</td><td>%d</td><td>%dMB</td><td>%d</td><td>%d</td></tr>",
                    entry.getKey(),
                    metrics.rowCount,
                    metrics.totalSize / (1024 * 1024),
                    metrics.readCount,
                    metrics.writeCount
            ));
        }

        if (!hasTableData) {
            html.append("<tr><td colspan='5' class='no-data'>No table data available</td></tr>");
        }
        html.append("</table>");

        // 添加自动刷新功能
        html.append("<script>");
        html.append("setTimeout(function() { window.location.reload(); }, 5000);"); // 每5秒刷新一次
        html.append("</script>");

        html.append("</body></html>");
        return html.toString();
    }
}