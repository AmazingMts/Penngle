package cis5550.flame;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Vector;

import cis5550.kvs.KVSClient;
import cis5550.tools.*;

public class FlameContextImpl implements FlameContext {

    private static final Logger logger = Logger.getLogger(FlameContextImpl.class);
    private static final UUIDGenerator uuidGenerator = new UUIDGenerator();

    private String jarName;
    private KVSClient kvs;
    private StringBuilder output;
    private List<String> workers;

    public FlameContextImpl(String jarName, KVSClient kvs, List<String> workers) {
        this.jarName = jarName;
        this.kvs = kvs;
        this.output = new StringBuilder();
        this.workers = workers;
    }

    @Override
    public KVSClient getKVS() {
        return kvs;
    }

    @Override
    public void output(String s) {
        this.output.append(s);
    }

    @Override
    public FlameRDD parallelize(List<String> list) throws Exception {
        String tableName = uuidGenerator.generateUniqueID();
        for (int i = 0; i < list.size(); i++) {
            String key = Hasher.hash(String.valueOf(i));
            String value = list.get(i);
            kvs.put(tableName, key, "value", value);
        }
        return new FlameRDDImpl(tableName, this);
    }

    @Override
    public FlameRDD fromTable(String tableName, RowToString lambda) throws Exception {
        String op = "/context/fromTable";
        String outputTable = invokeOperation(tableName, op, Serializer.objectToByteArray(lambda));
        if (outputTable == null) {
            return null;
        } else {
            return new FlameRDDImpl(outputTable, this);
        }
    }

    public String getOutput() {
        return output.toString();
    }

    public String invokeOperation(String inputTable, String operation, byte[] lambda, FlamePair... extraParams) {
        String outputTable = uuidGenerator.generateUniqueID();
        Partitioner partitioner = new Partitioner();
        try {
            int numWorkers = kvs.numWorkers();

            for (int i = 0; i < numWorkers; i++) {
                if (i != numWorkers - 1) {
                    String workerAddress = kvs.getWorkerAddress(i);
                    String workerID = kvs.getWorkerID(i);
                    String nextWorkerID = kvs.getWorkerID(i + 1);
                    partitioner.addKVSWorker(workerAddress, workerID, nextWorkerID);
                } else {
                    // handle last worker
                    String workerAddress = kvs.getWorkerAddress(i);
                    String workerID = kvs.getWorkerID(i);
                    partitioner.addKVSWorker(workerAddress, workerID, null);
                    partitioner.addKVSWorker(workerAddress, null, workerID);
                }
            }

//            logger.info(" # of flame workers: " + workers.size());

            for (String workerAddress : workers) {
                partitioner.addFlameWorker(workerAddress);
            }

            Vector<Partitioner.Partition> partitions = partitioner.assignPartitions();

            List<Thread> threads = new ArrayList<>();
            for (Partitioner.Partition partition : partitions) {
                Thread thread = new Thread(() -> {
                    try {
                        String fromKey = partition.fromKey == null ? "" : partition.fromKey;
                        String toKey = partition.toKeyExclusive == null ? "" : partition.toKeyExclusive;

                        String url = "http://" + partition.assignedFlameWorker + operation;
                        StringBuilder queryString = new StringBuilder(
                                String.format("input=%s&output=%s&kvs=%s&fromKey=%s&toKey=%s",
                                URLEncoder.encode(inputTable, StandardCharsets.UTF_8),
                                URLEncoder.encode(outputTable, StandardCharsets.UTF_8),
                                URLEncoder.encode(kvs.getCoordinator(), StandardCharsets.UTF_8),
                                URLEncoder.encode(fromKey, StandardCharsets.UTF_8),
                                URLEncoder.encode(toKey, StandardCharsets.UTF_8)));

                        for (int i = 0; i < extraParams.length; i++) {
                            queryString.append("&").append(URLEncoder.encode(extraParams[i].a, StandardCharsets.UTF_8))
                                    .append("=").append(URLEncoder.encode(extraParams[i].b, StandardCharsets.UTF_8));
                        }

                        int result = lambda == null ?
                                HTTP.doRequest("POST", url + "?" + queryString, new byte[0]).statusCode() :
                                HTTP.doRequest("POST", url + "?" + queryString, lambda).statusCode();

                        if (result != 200) {
                            throw new Exception("Worker returned status " + result);
                        }
                    } catch (Exception _) {
                    }
                });
                threads.add(thread);
                thread.start();
            }
            for (Thread thread : threads) {
                thread.join();
            }
            return outputTable;
        } catch (Exception e) {
            logger.error(e.getMessage());
            return null;
        }
    }
    public int getWorkerCount() {return workers.size();}
}
