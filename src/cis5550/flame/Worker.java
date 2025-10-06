package cis5550.flame;

import java.util.*;
import java.io.*;

import static cis5550.webserver.Server.*;
import cis5550.tools.Logger;
import cis5550.tools.Serializer;
import cis5550.kvs.*;

class Worker extends cis5550.generic.Worker {

    private static final Logger logger = Logger.getLogger(Worker.class);
    private static final UUIDGenerator uuidGenerator = new UUIDGenerator();
    private static File myJar;

	public static void main(String[] args) {
        if (args.length != 2) {
            System.err.println("Syntax: Worker <port> <coordinatorIP:port>");
            System.exit(1);
        }

        int port = Integer.parseInt(args[0]);
        String[] serverParts = args[1].split(":");
        String coordinatorIP = serverParts[0];
        int coordinatorPort = Integer.parseInt(serverParts[1]);
        startPingThread(coordinatorIP, coordinatorPort, ""+port, port);
        final File myJAR = new File("__worker"+port+"-current.jar");
        myJar = myJAR;

        port(port);

        post("/useJAR", (request,response) -> {
          FileOutputStream fos = new FileOutputStream(myJAR);
          fos.write(request.bodyAsBytes());
          fos.close();
          return "OK";
        });

        postForRDDFlatMap();
        postForRDDMapToPair();
        postForPairRDDFoldByKey();
        postForFromTable();
        postForRDDFlatMapToPair();
        postForPairRDDFlatMap();
        postForPairRDDFlatMapToPair();
        postForRDDDistinct();
        postForPairRDDJoin();
        postForRDDFold();
        postForRDDFilter();
    }

    private static String partitionFunction(String key) {
        if (key == null) {
            return "0";
        }
        return String.valueOf(key.hashCode() % 10);
    }

    private static KVSClient createBufferedKVSClient(String coordinator) {
        return new KVSClient(coordinator, 1000, 30);
    }

    private static class RowEntry {
        final String table;
        final String row;
        final String column;
        final byte[] value;

        private RowEntry(String tableName, String rowName, String columnName, byte[] value) {
            if (tableName == null || rowName == null || columnName == null || value == null) {
                throw new IllegalArgumentException("RowEntry fields cannot be null");
            }
            this.table = tableName;
            this.row = rowName;
            this.column = columnName;
            this.value = value;
        }
    }

    private static void postForRDDFlatMap() {
        post("/rdd/flatMap", (request, response) -> {
            KVSClient kvs = null;
            try {
                String inputTable = request.queryParams("input");
                String outputTable = request.queryParams("output");
                String kvsCoordinator = request.queryParams("kvs");
                String fromKey = "".equals(request.queryParams("fromKey")) ? null : request.queryParams("fromKey");
                String toKey = "".equals(request.queryParams("toKey")) ? null : request.queryParams("toKey");

                kvs = createBufferedKVSClient(kvsCoordinator);

                Map<String, List<RowEntry>> buffer = new HashMap<>();

                FlameRDD.StringToIterable lambda = (FlameRDD.StringToIterable)
                        Serializer.byteArrayToObject(request.bodyAsBytes(), myJar);

                Iterator<Row> iter = kvs.scan(inputTable, fromKey, toKey);
                while (iter.hasNext()) {
                    Row row = iter.next();
                    String value = row.get("value");
                    Iterable<String> results = lambda.op(value);
                    if (results != null) {
                        for (String result : results) {
                            String rowKey = uuidGenerator.generateUniqueID();
                            if (rowKey != null) {
                                String partitionKey = partitionFunction(rowKey);
                                buffer.computeIfAbsent(partitionKey, k -> new ArrayList<>())
                                        .add(new RowEntry(outputTable, rowKey, "value", result.getBytes()));
                            }
                        }
                    }
                }

                // Batch write for each partition
                for (Map.Entry<String, List<RowEntry>> entry : buffer.entrySet()) {
                    List<RowEntry> partitionData = entry.getValue();
                    for (RowEntry rowEntry : partitionData) {
                        kvs.bufferedPut(rowEntry.table, rowEntry.row, rowEntry.column, rowEntry.value);
                    }
                }
                kvs.flushPutBuffer();
            } catch (Exception e) {
                response.status(400, "Bad Request");
                return "ERROR";
            } finally {
                if (kvs != null) {
                    try {
                        kvs.closeBuffer();
                    } catch (Exception e) {
                        logger.error("Error closing KVS client", e);
                    }
                }
            }
            response.status(200, "OK");
            return null;
        });
    }

    private static void postForRDDMapToPair() {
        post("/rdd/mapToPair", (request, response) -> {
            KVSClient kvs = null;
            try {
                String inputTable = request.queryParams("input");
                String outputTable = request.queryParams("output");
                String kvsCoordinator = request.queryParams("kvs");
                String fromKey = "".equals(request.queryParams("fromKey")) ? null : request.queryParams("fromKey");
                String toKey = "".equals(request.queryParams("toKey")) ? null : request.queryParams("toKey");
                kvs = createBufferedKVSClient(kvsCoordinator);

                Map<String, List<RowEntry>> buffer = new HashMap<>();

                FlameRDD.StringToPair lambda = (FlameRDD.StringToPair)
                        Serializer.byteArrayToObject(request.bodyAsBytes(), myJar);

                Iterator<Row> iter = kvs.scan(inputTable, fromKey, toKey);
                while (iter.hasNext()) {
                    Row row = iter.next();
                    String value = row.get("value");
                    FlamePair pair = lambda.op(value);
                    if (pair != null) {
                        if (pair._1() != null) {
                            String partitionKey = partitionFunction(pair._1());
                            buffer.computeIfAbsent(partitionKey, k -> new ArrayList<>())
                                    .add(new RowEntry(outputTable, pair._1(), row.key(), pair._2().getBytes()));
                        }
                    }
                }

                // Batch write for each partition
                for (Map.Entry<String, List<RowEntry>> entry : buffer.entrySet()) {
                    List<RowEntry> partitionData = entry.getValue();
                    for (RowEntry rowEntry : partitionData) {
                        kvs.bufferedPut(rowEntry.table, rowEntry.row, rowEntry.column, rowEntry.value);
                    }
                }
                kvs.flushPutBuffer();
            } catch (Exception e) {
                return "ERROR";
            } finally {
                if (kvs != null) {
                    try {
                        kvs.closeBuffer();
                    } catch (Exception e) {
                        logger.error("Error closing KVS client", e);
                    }
                }
            }
            response.status(200, "OK");
            return null;
        });
    }

    private static void postForPairRDDFoldByKey() {
        post("/pairrdd/foldByKey", (request, response) -> {
            KVSClient kvs = null;
            try {
                String inputTable = request.queryParams("input");
                String outputTable = request.queryParams("output");
                String kvsCoordinator = request.queryParams("kvs");
                String fromKey = "".equals(request.queryParams("fromKey")) ? null : request.queryParams("fromKey");
                String toKey = "".equals(request.queryParams("toKey")) ? null : request.queryParams("toKey");
                String zeroElement = request.queryParams("zeroElement");

                kvs = createBufferedKVSClient(kvsCoordinator);

                FlamePairRDD.TwoStringsToString lambda = (FlamePairRDD.TwoStringsToString)
                        Serializer.byteArrayToObject(request.bodyAsBytes(), myJar);

                Iterator<Row> iter = kvs.scan(inputTable, fromKey, toKey);
                while (iter.hasNext()) {
                    Row row = iter.next();
                    String accumulator = zeroElement;
                    for (String col : row.columns()) {
                        accumulator = lambda.op(accumulator, row.get(col));
                    }
                    kvs.bufferedPut(outputTable, row.key(), "value", accumulator);
                }
                kvs.flushPutBuffer(); // Moved outside of finally
            } catch (Exception e) {
                return "ERROR";
            } finally {
                if (kvs != null) {
                    try {
                        kvs.closeBuffer();
                    } catch (Exception e) {
                        logger.error("Error closing KVS client", e);
                    }
                }
            }
            response.status(200, "OK");
            return null;
        });
    }

    private static void postForFromTable() {
        post("/context/fromTable", (request, response) -> {
            KVSClient kvs = null;
            try {
                String inputTable = request.queryParams("input");
                String outputTable = request.queryParams("output");
                String kvsCoordinator = request.queryParams("kvs");
                String fromKey = "".equals(request.queryParams("fromKey")) ? null : request.queryParams("fromKey");
                String toKey = "".equals(request.queryParams("toKey")) ? null : request.queryParams("toKey");

                kvs = createBufferedKVSClient(kvsCoordinator);
                FlameContext.RowToString lambda = (FlameContext.RowToString)
                        Serializer.byteArrayToObject(request.bodyAsBytes(), myJar);

                Iterator<Row> iter = kvs.scan(inputTable, fromKey, toKey);
                while (iter.hasNext()) {
                    Row row = iter.next();
                    String result = lambda.op(row);
                    if (result != null) {
                        kvs.bufferedPut(outputTable, row.key(), "value", result);
                    }
                }
                kvs.flushPutBuffer();
            } catch (Exception e) {
                throw new RuntimeException(e);
            } finally {
                if (kvs != null) {
                    try {

                        kvs.closeBuffer();
                    } catch (Exception e) {
                        logger.error("Error closing KVS client", e);
                    }
                }
            }
            response.status(200, "OK");
            return null;
        });
    }

    private static void postForRDDFlatMapToPair() {
        post("/rdd/flatMapToPair", (request, response) -> {
            KVSClient kvs = null;
            try {
                String inputTable = request.queryParams("input");
                String outputTable = request.queryParams("output");
                String kvsCoordinator = request.queryParams("kvs");
                String fromKey = "".equals(request.queryParams("fromKey")) ? null : request.queryParams("fromKey");
                String toKey = "".equals(request.queryParams("toKey")) ? null : request.queryParams("toKey");

                kvs = createBufferedKVSClient(kvsCoordinator);

                Map<String, List<RowEntry>> buffer = new HashMap<>();

                FlameRDD.StringToPairIterable lambda = (FlameRDD.StringToPairIterable)
                        Serializer.byteArrayToObject(request.bodyAsBytes(), myJar);

                Iterator<Row> iter = kvs.scan(inputTable, fromKey, toKey);

                while (iter.hasNext()) {
                    Row row = iter.next();
                    for (String col : row.columns()) {
                        String value = row.get(col);
                        Iterable<FlamePair> fpIter = lambda.op(value);
                        for (FlamePair fp : fpIter) {
                            String newCol = uuidGenerator.generateUniqueID();
                            if (newCol != null) {
                                String partitionKey = partitionFunction(fp._1());
                                buffer.computeIfAbsent(partitionKey, k -> new ArrayList<>())
                                        .add(new RowEntry(outputTable, fp._1(), newCol, fp._2().getBytes()));
                            }
                        }
                    }
                }
                for (Map.Entry<String, List<RowEntry>> entry : buffer.entrySet()) {
                    List<RowEntry> partitionData = entry.getValue();
                    for (RowEntry rowEntry : partitionData) {
                        kvs.bufferedPut(rowEntry.table, rowEntry.row, rowEntry.column, rowEntry.value);
                    }
                }
                kvs.flushPutBuffer();
            } catch (Exception e) {
                response.status(400, "Bad Request");
                return "ERROR";
            } finally {
                if (kvs != null) {
                    try {

                        kvs.closeBuffer();
                    } catch (Exception e) {
                        logger.error("Error closing KVS client", e);
                    }
                }
            }
            response.status(200, "OK");
            return null;
        });
    }

    private static void postForPairRDDFlatMap() {
        post("/pairrdd/flatMap", (request, response) -> {
            KVSClient kvs = null;
            try {
                String inputTable = request.queryParams("input");
                String outputTable = request.queryParams("output");
                String kvsCoordinator = request.queryParams("kvs");
                String fromKey = "".equals(request.queryParams("fromKey")) ? null : request.queryParams("fromKey");
                String toKey = "".equals(request.queryParams("toKey")) ? null : request.queryParams("toKey");

                kvs = createBufferedKVSClient(kvsCoordinator);
                Map<String, List<RowEntry>> buffer = new HashMap<>();

                FlamePairRDD.PairToStringIterable lambda = (FlamePairRDD.PairToStringIterable)
                        Serializer.byteArrayToObject(request.bodyAsBytes(),myJar);
                Iterator<Row> iter = kvs.scan(inputTable, fromKey, toKey);

                while (iter.hasNext()) {
                    Row row = iter.next();
                    for (String col : row.columns()) {
                        FlamePair pair = new FlamePair(row.key(), row.get(col));
                        Iterable<String> strings = lambda.op(pair);
                        if (strings != null) {
                            for (String str : strings) {
                                if (str != null) {
                                    String newKey = uuidGenerator.generateUniqueID();
                                    if (newKey != null) {
                                        String partitionKey = partitionFunction(newKey);
                                        buffer.computeIfAbsent(partitionKey, k -> new ArrayList<>())
                                                .add(new RowEntry(outputTable, newKey, "value", str.getBytes()));
                                    }
                                }
                            }
                        }
                    }
                }

                // Batch write for each partition
                for (Map.Entry<String, List<RowEntry>> entry : buffer.entrySet()) {
                    List<RowEntry> partitionData = entry.getValue();
                    for (RowEntry rowEntry : partitionData) {
                        kvs.bufferedPut(rowEntry.table, rowEntry.row, rowEntry.column, rowEntry.value);
                    }
                }
                kvs.flushPutBuffer();
            } catch (Exception e) {
                return "ERROR: " + e.getMessage();
            } finally {
                if (kvs != null) {
                    try {
                        kvs.closeBuffer();
                    } catch (Exception e) {
                        logger.error("Error closing KVS client", e);
                    }
                }
            }
            response.status(200, "OK");
            return null;
        });
    }

    private static void postForPairRDDFlatMapToPair() {
        post("/pairrdd/flatMapToPair", (request, response) -> {
            KVSClient kvs = null;
            try {
                String inputTable = request.queryParams("input");
                String outputTable = request.queryParams("output");
                String kvsCoordinator = request.queryParams("kvs");
                String fromKey = "".equals(request.queryParams("fromKey")) ? null : request.queryParams("fromKey");
                String toKey = "".equals(request.queryParams("toKey")) ? null : request.queryParams("toKey");

                kvs = createBufferedKVSClient(kvsCoordinator);
                Map<String, List<RowEntry>> buffer = new HashMap<>();

                FlamePairRDD.PairToPairIterable lambda = (FlamePairRDD.PairToPairIterable)
                        Serializer.byteArrayToObject(request.bodyAsBytes(), myJar);
                Iterator<Row> iter = kvs.scan(inputTable, fromKey, toKey);

                while (iter.hasNext()) {
                    Row row = iter.next();
                    for (String col : row.columns()) {
                        FlamePair pair = new FlamePair(row.key(), row.get(col));
                        Iterable<FlamePair> pairs = lambda.op(pair);
                        for (FlamePair p : pairs) {
                            String newKey = uuidGenerator.generateUniqueID();
                            if (newKey != null) {
                                String partitionKey = partitionFunction(p._1());
                                buffer.computeIfAbsent(partitionKey, k -> new ArrayList<>())
                                        .add(new RowEntry(outputTable, p._1(), newKey, p._2().getBytes()));
                            }
                        }
                    }
                }

                // Batch write for each partition
                for (Map.Entry<String, List<RowEntry>> entry : buffer.entrySet()) {
                    List<RowEntry> partitionData = entry.getValue();
                    for (RowEntry rowEntry : partitionData) {
                        kvs.bufferedPut(rowEntry.table, rowEntry.row, rowEntry.column, rowEntry.value);
                    }
                }
                kvs.flushPutBuffer();
            } catch (Exception e) {
                return "ERROR: " + e.getMessage();
            } finally {
                if (kvs != null) {
                    try {
                        kvs.closeBuffer();
                    } catch (Exception e) {
                        logger.error("Error closing KVS client", e);
                    }
                }
            }
            response.status(200, "OK");
            return null;
        });
    }

    private static void postForRDDDistinct() {
        post("/rdd/distinct", (request, response) -> {
            KVSClient kvs = null;
            try {
                String inputTable = request.queryParams("input");
                String outputTable = request.queryParams("output");
                String kvsCoordinator = request.queryParams("kvs");
                String fromKey = "".equals(request.queryParams("fromKey")) ? null : request.queryParams("fromKey");
                String toKey = "".equals(request.queryParams("toKey")) ? null : request.queryParams("toKey");

                kvs = createBufferedKVSClient(kvsCoordinator);
                Iterator<Row> iter = kvs.scan(inputTable, fromKey, toKey);
                while (iter.hasNext()) {
                    Row row = iter.next();
                    for (String col : row.columns()) {
                        String value = row.get(col);
                        kvs.bufferedPut(outputTable, value, "value", value);
                    }
                }
                kvs.flushPutBuffer();
            } catch (Exception e) {
                logger.error("Error in distinct operation", e);
                response.status(500, "Internal Server Error");
                return "ERROR: " + e.getMessage();
            } finally {
                if (kvs != null) {
                    try {
                        kvs.closeBuffer();
                    } catch (Exception e) {
                        logger.error("Error closing KVS client", e);
                    }
                }
            }
            response.status(200, "OK");
            return null;
        });
    }

    private static void postForPairRDDJoin() {
        post("/pairrdd/join", (request, response) -> {
            KVSClient kvs = null;
            try {
                String inputTable = request.queryParams("input");
                String otherTable = request.queryParams("otherTable");
                String outputTable = request.queryParams("output");
                String kvsCoordinator = request.queryParams("kvs");
                String fromKey = "".equals(request.queryParams("fromKey")) ? null : request.queryParams("fromKey");
                String toKey = "".equals(request.queryParams("toKey")) ? null : request.queryParams("toKey");

                kvs = createBufferedKVSClient(kvsCoordinator);

                Iterator<Row> iter = kvs.scan(inputTable, fromKey, toKey);
                Iterator<Row> otherIter = kvs.scan(otherTable, fromKey, toKey);

                Map<String, Row> otherTableRowMap = new HashMap<>();
                while (otherIter.hasNext()) {
                    Row row = otherIter.next();
                    otherTableRowMap.put(row.key(), row);
                }

                while (iter.hasNext()) {
                    Row row = iter.next();
                    Row otherRow = otherTableRowMap.get(row.key());
                    if (otherRow != null) {
                        for (String col : row.columns()) {
                            for (String otherCol : otherRow.columns()) {
                                kvs.bufferedPut(outputTable, row.key(), uuidGenerator.generateUniqueID(), row.get(col)
                                + "," + otherRow.get(otherCol));
                            }
                        }
                    }
                }
                kvs.flushPutBuffer();
            } catch (Exception e) {
                logger.error("Error in join operation", e);
                response.status(500, "Internal Server Error");
                return "ERROR: " + e.getMessage();
            } finally {
                if (kvs != null) {
                    try {
                        kvs.closeBuffer();
                    } catch (Exception e) {
                        logger.error("Error closing KVS client", e);
                    }
                }
            }
            response.status(200, "OK");
            return null;
        });
    }

    private static void postForRDDFold() {
        post("/rdd/fold", (request, response) -> {
            KVSClient kvs = null;
            try {
                String inputTable = request.queryParams("input");
                String outputTable = request.queryParams("output");
                String kvsCoordinator = request.queryParams("kvs");
                String fromKey = "".equals(request.queryParams("fromKey")) ? null : request.queryParams("fromKey");
                String toKey = "".equals(request.queryParams("toKey")) ? null : request.queryParams("toKey");
                String zeroElement = request.queryParams("zeroElement");

                kvs = createBufferedKVSClient(kvsCoordinator);

                FlamePairRDD.TwoStringsToString lambda = (FlamePairRDD.TwoStringsToString)
                        Serializer.byteArrayToObject(request.bodyAsBytes(), myJar);

                Iterator<Row> iter = kvs.scan(inputTable, fromKey, toKey);
                String accumulator = zeroElement;
                while (iter.hasNext()) {
                    Row row = iter.next();
                    for (String col : row.columns()) {
                        accumulator = lambda.op(accumulator, row.get(col));
                    }
                    kvs.bufferedPut(outputTable, row.key(), "value", accumulator);
                }
                kvs.flushPutBuffer();
            } catch (Exception e) {
                return "ERROR";
            } finally {
                if (kvs != null) {
                    try {
                        kvs.closeBuffer();
                    } catch (Exception e) {
                        logger.error("Error closing KVS client", e);
                    }
                }
            }
            response.status(200, "OK");
            return null;
        });
    }

    private static void postForRDDFilter() {
        post("/rdd/filter", (request, response) -> {
            KVSClient kvs = null;
            try {
                String inputTable = request.queryParams("input");
                String outputTable = request.queryParams("output");
                String kvsCoordinator = request.queryParams("kvs");
                String fromKey = "".equals(request.queryParams("fromKey")) ? null : request.queryParams("fromKey");
                String toKey = "".equals(request.queryParams("toKey")) ? null : request.queryParams("toKey");

                kvs = createBufferedKVSClient(kvsCoordinator);

                FlameRDD.StringToBoolean lambda = (FlameRDD.StringToBoolean)
                        Serializer.byteArrayToObject(request.bodyAsBytes(), myJar);
                Iterator<Row> iter = kvs.scan(inputTable, fromKey, toKey);
                while (iter.hasNext()) {
                    Row row = iter.next();
                    String value = row.get("value");
                    if (value != null) {
                        boolean keep = lambda.op(value);
                        if (keep) {
                            kvs.bufferedPut(outputTable, row.key(), "value", value);
                        }
                    }
                }
                kvs.flushPutBuffer();
            } catch (Exception e) {
                return "ERROR";
            } finally {
                if (kvs != null) {
                    try {
                        kvs.closeBuffer();
                    } catch (Exception e) {
                        logger.error("Error closing KVS client", e);
                    }
                }
            }
            response.status(200, "OK");
            return "OK";
        });
    }
}
