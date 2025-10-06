package cis5550.kvs;

import cis5550.test.KvsBenchmark;
import cis5550.tools.KeyEncoder;
import cis5550.tools.Logger;
import cis5550.webserver.Server;

import static cis5550.webserver.Server.*;
import java.io.*;
import java.lang.management.ManagementFactory;
import java.net.URLEncoder;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Predicate;
import java.util.stream.Stream;

public class Worker extends cis5550.generic.Worker {

    private static final Logger logger = Logger.getLogger(Worker.class);

    private static TableHandler tableHandler;

    private static String storageDirectory = "";
    private static String workerID = "";

    private static final MonitoringSystem monitoringSystem = new MonitoringSystem();

    public static void main(String[] args) {
        String[] coordinatorInfo = {};
        int portNumber = 0;
        String coordinatorIP = "";
        int coordinatorPort = 0;
        try {
            portNumber = Integer.parseInt(args[0]);
            storageDirectory = args[1];
            coordinatorInfo = args[2].split(":");
            coordinatorIP = coordinatorInfo[0];
            coordinatorPort = Integer.parseInt(coordinatorInfo[1]);
        } catch (NumberFormatException e) {
            logger.info("Error with kvs Worker: " + e.getMessage());
            System.exit(-1);
        }

        port(portNumber);

        File workerDir = new File(storageDirectory);
        if (!workerDir.exists()) {
            workerDir.mkdirs();
        }

        try {
            tableHandler = new TableHandler(storageDirectory);
        } catch (Exception e) {
            System.exit(-1);
        }

        File idFile = new File(storageDirectory + "/id");
        String id = "";

        // If the ID file exists, read the worker ID from it
        if (idFile.exists() && idFile.isFile()) {
            try (BufferedReader reader = new BufferedReader(new FileReader(idFile))) {
                id = reader.readLine();
            } catch (IOException e) {
                System.exit(-1);
            }
        } else {
            // If the ID file does not exist, generate a new worker ID
            id = generateRandomId();
            // Write the new worker ID to the file
            try (BufferedWriter writer = new BufferedWriter(new FileWriter(idFile))) {
                writer.write(id);
            } catch (IOException e) {
                logger.info(" Error writing to file: " + e.getMessage());
                System.exit(-1);
            }
        }

        workerID = id;

        startPingThread(coordinatorIP, coordinatorPort, id, portNumber);

        setupMonitoringEndpoints();

        putDataToTable();
        getDataFromTable();
        getTableInfo();
        getListOfRowsFromTable();
        getRowFromTable();
        getTable();
        putTable();
        putPersistentTable();
        putRowsInTable();
        getRowCountOfTable();
        renameTable();
        deleteTable();
    }

    public static String generateRandomId() {
        Random random = new Random();
        StringBuilder id = new StringBuilder();
        for (int i = 0; i < 5; i++) {
            id.append((char) ('a' + random.nextInt(26)));
        }
        return id.toString();
    }

    private static String getId() {return workerID;}

    private static void setupMonitoringEndpoints() {
        get("/metrics", (request, response) -> {
            response.type("text/html");
            return monitoringSystem.generateHTMLReport();
        });

        get("/status", (request, response) -> {
            // Get system metrics
            Runtime runtime = Runtime.getRuntime();
            double cpuUsage = ManagementFactory.getOperatingSystemMXBean().getSystemLoadAverage();
            long memoryUsage = runtime.totalMemory() - runtime.freeMemory();

            // Record metrics
            monitoringSystem.recordWorkerHeartbeat(getId(), cpuUsage, memoryUsage);

            response.type("application/json");
            return String.format("{\"status\":\"healthy\",\"cpu\":%.2f,\"memory\":%d}",
                    cpuUsage, memoryUsage);
        });
    }

//    private static void setupBenchMark() {
//        KvsBenchmark benchmark = new KvsBenchmark();
//
//        get("/benchmark", ((request, response) -> {
//            response.type("text/html");
//            return
//        }));
//    }

    public static void putDataToTable() {
        put("/data/:T/:R/:C", (req, res) -> {
            long startTime = System.currentTimeMillis();
            String tableName = req.params("T");
            String rowKey = req.params("R");
            String columnKey = req.params("C");
            byte[] data = req.bodyAsBytes();

            try {
                Row row = new Row(rowKey);
                row.put(columnKey, data);
                tableHandler.putRow(tableName, row);
                monitoringSystem.recordTableOperation(tableName, "write", req.bodyAsBytes().length);
                monitoringSystem.recordWorkerHeartbeat(getId(),
                        ManagementFactory.getOperatingSystemMXBean().getSystemLoadAverage(),
                        Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory()
                );
                res.status(200, "OK");
                return "OK";
            } catch (Exception e) {
                return "ERROR";
            } finally {
                long responseTime = System.currentTimeMillis() - startTime;
                MonitoringSystem.WorkerMetrics metrics = monitoringSystem.workerMetrics.get(getId());
                if (metrics != null) {
                    metrics.recordRequest(req.bodyAsBytes().length, responseTime, false);
                }
            }
        });
    }

    public static void getDataFromTable() {
        get("/data/:T/:R/:C", (req, res) -> {
            String tableName = req.params("T");
            String rowKey = req.params("R");
            String columnKey = req.params("C");

            if (tableName == null) {
                res.status(404, "Not Found");
                return null;
            }
            Row row = tableHandler.getRow(tableName, rowKey);
            byte[] data = null;
            if (row == null) {
                res.status(404, "Not Found");
                return "NOT FOUND";
            } else {
                data = row.getBytes(columnKey);
            }
            if (data == null) {
                res.status(404, "Not Found");
                return "NOT FOUND";
            }
            res.bodyAsBytes(data);
            return null;
        });
    }

    public static void getTableInfo() {
        get("/", (req, res) -> {
            StringBuilder html = new StringBuilder();
            html.append("<html><body><h1>Table List</h1>");
            html.append("<table border='1'>");
            html.append("<tr><th>Name</th><th>Keys</th></tr>");
            for (Map.Entry<String, ConcurrentHashMap<String, Row>> entry : tableHandler.getInMemoryTableMap().entrySet()) {
                String tableName = entry.getKey();
                int rowCount = entry.getValue().size();
                html.append("<tr>")
                        .append("<td><a href='/view/").append(tableName).append("'>")
                        .append(tableName).append("</a></td>")
                        .append("<td>").append(rowCount).append("</td>")
                        .append("</tr>");
            }
            if (tableHandler != null) {
                try {
                    File[] files = new File(storageDirectory).listFiles();
                    if (files != null) {
                        for (File file : files) {
                            if (file.isFile() && !file.getName().equals("id")) {
                                String tableName = file.getName();
                                PersistentTable table = tableHandler.getPersistentTable(tableName);
                                int rowCount = table.size();
                                html.append("<tr>")
                                        .append("<td><a href='/view/").append(tableName).append("'>")
                                        .append(tableName).append("</a></td>")
                                        .append("<td>").append(rowCount).append("</td>")
                                        .append("</tr>");
                            }
                        }
                    }
                } catch (Exception e) {
                    logger.error("Error reading persistent table", e);
                }
            }

            html.append("</table></body></html>");
            res.type("text/html");
            res.status(200, "OK");
            return html.toString();
        });
    }

    public static void getListOfRowsFromTable() {
        get("/view/:tableName", (req, res) -> {
            String tableName = req.params("tableName");
            String fromRow = req.queryParams("fromRow");
            int pageSize = 10;

            StringBuilder html = new StringBuilder();
            html.append("<head><title>Table view</title></head>");
            html.append("<body><h3>Table: ").append(tableName).append("</h3>");
            html.append("<table border='1'>\n");

            TreeSet<String> rowKeys = new TreeSet<>();
            TreeSet<String> columnNames = new TreeSet<>();

            try {
                if (tableName.startsWith("pt-")) {
                    PersistentTable table = tableHandler.getPersistentTable(tableName);
                    for (Enumeration<String> e = table.getKeys(); e.hasMoreElements();) {
                        String key = e.nextElement();
                        if (fromRow == null || key.compareTo(fromRow) > 0) {
                            rowKeys.add(key);
                        }
                    }
                } else {
                    ConcurrentHashMap<String, Row> table = tableHandler.getInMemoryTable(tableName);
                    if (table != null) {
                        for (String key : table.keySet()) {
                            if (fromRow == null || key.compareTo(fromRow) > 0) {
                                rowKeys.add(key);
                            }
                        }
                    }
                }

                Iterator<String> keyIter = rowKeys.iterator();
                int count = 0;
                while (keyIter.hasNext() && count < pageSize) {
                    String key = keyIter.next();
                    Row row = tableHandler.getRow(tableName, key);
                    if (row != null) {
                        columnNames.addAll(row.columns());
                    }
                    count++;
                }

                html.append("<tr><th>Key</th>");
                for (String column : columnNames) {
                    html.append("<th>").append(column).append("</th>");
                }
                html.append("</tr>\n");

                keyIter = rowKeys.iterator();
                count = 0;
                String nextKey = null;
                while (keyIter.hasNext() && count < pageSize) {
                    String key = keyIter.next();
                    Row row = tableHandler.getRow(tableName, key);
                    if (row != null) {
                        html.append("<tr><td>").append(key).append("</td>");
                        for (String column : columnNames) {
                            String value = row.get(column);
                            html.append("<td>").append(value != null ? value : "").append("</td>");
                        }
                        html.append("</tr>\n");
                    }
                    count++;
                    if (keyIter.hasNext() && count == pageSize) {
                        nextKey = keyIter.next();
                    }
                }

                html.append("</table>");

                if (nextKey != null) {
                    html.append("<p><a href='/view/").append(tableName)
                            .append("?fromRow=").append(URLEncoder.encode(nextKey, "UTF-8"))
                            .append("'>next</a></p>");
                }

            } catch (Exception e) {
                logger.error("Error showing content of table", e);
                return "Error：" + e.getMessage();
            }

            html.append("</body></html>");
            res.type("text/html");
            return html.toString();
        });
    }

    public static void putPersistentTable() {
        put("/persist/:T", (req, res) -> {
            String tableName = req.params("T");
            if (tableHandler.containsKey(tableName)) {
                res.status(403, "FORBIDDEN");
                return "FORBIDDEN";
            } else {
                tableHandler.createTableIfNecessary(tableName);
                res.status(200, "OK");
                return "OK";
            }
        });
    }

    public static void getRowFromTable() {
        get("/data/:T/:R", (req, res) -> {
            String tableName = req.params("T");
            String rowKey = req.params("R");
            if (tableHandler.containsKey(tableName)) {
                if (tableName.startsWith("pt-")) {
                    PersistentTable table = tableHandler.getPersistentTable(tableName);
                    if (table.containsKey(rowKey)) {
                        Row row = table.getRow(rowKey);
                        res.bodyAsBytes(row.toByteArray());
                        res.status(200, "OK");
                        return null;
                    } else {
                        res.status(404, "NOT FOUND");
                        return "NOT FOUND";
                    }
                } else {
                    ConcurrentHashMap<String, Row> table = tableHandler.getInMemoryTable(tableName);
                    if (table.containsKey(rowKey)) {
                        Row row = table.get(rowKey);
                        res.bodyAsBytes(row.toByteArray());
                        res.status(200, "OK");
                        return null;
                    } else {
                        res.status(404, "NOT FOUND");
                        return "NOT FOUND";
                    }
                }
            } else {
                res.status(404, "NOT FOUND");
                return "NOT FOUND";
            }
        });
    }

    public static void getTable() {
        get("/data/:T", (req, res) -> {
            String tableName = req.params("T");
            String startRow = req.queryParams("startRow");
            String endRowExclusive = req.queryParams("endRowExclusive");

            if (!tableHandler.containsKey(tableName)) {
                res.status(404, "NOT FOUND");
                return "NOT FOUND";
            }

            if (tableName.startsWith("pt-")) {
                PersistentTable table = tableHandler.getPersistentTable(tableName);
                for (Enumeration<String> e = table.getKeys(); e.hasMoreElements(); ) {
                    String rowKey = e.nextElement();
                    if ((startRow == null || startRow.isEmpty() || startRow.compareTo(rowKey) < 0) &&
                    (endRowExclusive == null || endRowExclusive.isEmpty() || endRowExclusive.compareTo(rowKey) > 0)) {
                        Row row = table.getRow(rowKey);
                        res.write(row.toByteArray());
                        res.write("\n".getBytes());
                    }
                }
                res.write("\n".getBytes());
            } else {
                ConcurrentHashMap<String, Row> table = tableHandler.getInMemoryTable(tableName);
                if (table != null) {
                    for (String rowKey : table.keySet()) {
                        if ((startRow != null && rowKey.compareTo(startRow) < 0) ||
                                (endRowExclusive != null && rowKey.compareTo(endRowExclusive) >= 0)) {
                            continue;
                        }
                        Row row = table.get(rowKey);
                        res.write(row.toByteArray());
                        res.write("\n".getBytes());
                    }
                    res.write("\n".getBytes());
                }
            }
            res.status(200, "OK");
            return null;
        });
    }

    public static void putTable() {
        put("/data/:T", (req, res) -> {
            String tableName = req.params("T");
            tableHandler.createTableIfNecessary(tableName);
            InputStream is = new ByteArrayInputStream(req.bodyAsBytes());
            while (is.available() > 0) {
                Row row = Row.readFrom(is);
                if (row != null) {
                    tableHandler.putRow(tableName, row);
                }
            }
            res.status(200, "OK");
            return "OK";
        });
    }

    private static void putRowsInTable() {
        put("/data/put-rows/:T", (req, res) -> {
            long startTime = System.currentTimeMillis();
            String tableName = req.params("T");

            try {
                InputStream is = new ByteArrayInputStream(req.bodyAsBytes());
                List<Row> rows = new ArrayList<>();
                while (is.available() > 0) {
                    Row row = Row.readFrom(is);
                    if (row != null) {
                        rows.add(row);
                    }
                }
                tableHandler.createTableIfNecessary(tableName);
                tableHandler.putRows(tableName, rows);
                monitoringSystem.recordTableOperation(tableName, "write", req.bodyAsBytes().length);
                monitoringSystem.recordWorkerHeartbeat(getId(),
                        ManagementFactory.getOperatingSystemMXBean().getSystemLoadAverage(),
                        Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory()
                );
                res.status(200, "OK");
                return "OK";
            } catch (Exception e) {
                return "ERROR";
            } finally {
                long responseTime = System.currentTimeMillis() - startTime;
                MonitoringSystem.WorkerMetrics metrics = monitoringSystem.workerMetrics.get(getId());
                if (metrics != null) {
                    metrics.recordRequest(req.bodyAsBytes().length, responseTime, false);
                }
            }
        });
    }

    public static void getRowCountOfTable() {
        get("/count/:T", (req, res) -> {
            String tableName = req.params("T");
            if (tableHandler.containsKey(tableName)) {
                if (tableName.startsWith("pt-")) {
                    PersistentTable table = tableHandler.getPersistentTable(tableName);
                    int count = table.size();
                    res.bodyAsBytes(String.valueOf(count).getBytes());
                    res.status(200, "OK");
                    return null;
                } else {
                    ConcurrentHashMap<String, Row> table = tableHandler.getInMemoryTable(tableName);
                    int count = table.size();
                    res.bodyAsBytes(String.valueOf(count).getBytes());
                    res.status(200, "OK");
                    return null;
                }
            } else {
                res.status(404, "NOT FOUND");
                return "NOT FOUND";
            }
        });
    }

    public static void renameTable() {
        put("/rename/:T", (req, res) -> {
            String oldName = req.params("T");
            String newName = req.body().trim();
            synchronized (tableHandler) {
                if (tableHandler.containsKey(oldName)) {
                    try {
                        tableHandler.rename(oldName, newName);
                        res.status(200, "OK");
                        return "OK";
                    } catch (FileAlreadyExistsException e) {
                        res.status(409, "Conflict");
                        return "Conflict";
                    } catch (IOException e) {
                        res.status(500, "Internal Server Error");
                        return "Internal Server Error";
                    }
                } else {
                    res.status(404, "NOT FOUND");
                    return "NOT FOUND";
                }
            }
        });
    }

    public static void deleteTable() {
        put("/delete/:T", (req, res) -> {
            String tableName = req.params("T");
            synchronized (tableHandler) {
                if (tableHandler.containsKey(tableName)) {
                    tableHandler.delete(tableName);
                    res.status(200, "OK");
                    return null;
                } else {
                    res.status(404, "NOT FOUND");
                    return "NOT FOUND";
                }
            }
        });
    }
}
