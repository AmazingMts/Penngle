package cis5550.flame;

import cis5550.kvs.Row;
import cis5550.kvs.KVSClient;
import cis5550.tools.Logger;
import cis5550.tools.Serializer;

import java.util.*;

public class FlameRDDImpl implements FlameRDD {

    private static final Logger logger = Logger.getLogger(FlameRDDImpl.class);

    private String tableName;
    private FlameContextImpl context;

    public FlameRDDImpl(String tableName, FlameContextImpl context) {
        this.tableName = tableName;
        this.context = context;
    }

    public String getTableName() {
        return tableName;
    }

    @Override
    public int count() throws Exception {
        return context.getKVS().count(tableName);
    }

    @Override
    public void saveAsTable(String tableNameArg) throws Exception {
        context.getKVS().rename(tableName, tableNameArg);
        tableName = tableNameArg;
    }

    @Override
    public FlameRDD distinct() throws Exception {
        String op = "/rdd/distinct";
        String outputTable = context.invokeOperation(tableName, op, null);
        if (outputTable == null) {
            return null;
        } else {
            return new FlameRDDImpl(outputTable, context);
        }
    }

    @Override
    public void destroy() throws Exception {
        context.getKVS().delete(tableName);
    }

    @Override
    public Vector<String> take(int num) throws Exception {
        Vector<String> result = new Vector<>();
        KVSClient kvs = context.getKVS();
        Iterator<Row> iter = kvs.scan(tableName);
        while (iter.hasNext() && result.size() < num) {
            Row row = iter.next();
            result.add(row.get("value"));
        }
        return result;
    }

    @Override
    public String fold(String zeroElement, FlamePairRDD.TwoStringsToString lambda) throws Exception {
        String op = "/rdd/fold";
        FlamePair extraParam = new FlamePair("zeroElement", zeroElement);
        String outputTable = context.invokeOperation(tableName, op, Serializer.objectToByteArray(lambda), extraParam);
        if (outputTable == null) {
            return null;
        }
        KVSClient kvs = context.getKVS();
        String finalResult = zeroElement;

        Iterator<Row> iter = kvs.scan(outputTable);
        while (iter.hasNext()) {
            Row row = iter.next();
            String workerResult = row.get("value");
            if (workerResult != null) {
                finalResult = lambda.op(finalResult, workerResult);
            }
        }
        kvs.delete(outputTable);

        return finalResult;
    }

    @Override
    public List<String> collect() throws Exception {
        List<String> result = new ArrayList<>();
        KVSClient kvs = context.getKVS();

        Iterator<Row> iter = kvs.scan(tableName);
        while (iter.hasNext()) {
            Row row = iter.next();
            String value = row.get("value");
            if (value != null) {
                result.add(value);
            }
        }
        return result;
    }

    @Override
    public FlameRDD flatMap(StringToIterable lambda) throws Exception {
        String op = "/rdd/flatMap";
        String outputTable = context.invokeOperation(tableName, op, Serializer.objectToByteArray(lambda));
        if (outputTable == null) {
            return null;
        } else {
            return new FlameRDDImpl(outputTable, context);
        }
    }

    @Override
    public FlamePairRDD flatMapToPair(StringToPairIterable lambda) throws Exception {
        String op = "/rdd/flatMapToPair";
        String outputTable = context.invokeOperation(tableName, op, Serializer.objectToByteArray(lambda));
        if (outputTable == null) {
            return null;
        } else {
            return new FlamePairRDDImpl(outputTable, context);
        }
    }

    @Override
    public FlamePairRDD mapToPair(StringToPair lambda) throws Exception {
        String op = "/rdd/mapToPair";
        String outputTable = context.invokeOperation(tableName, op, Serializer.objectToByteArray(lambda));
        if (outputTable == null) {
            return null;
        } else {
            return new FlamePairRDDImpl(outputTable, context);
        }
    }

    @Override
    public FlameRDD intersection(FlameRDD r) throws Exception {
        return null;
    }

    @Override
    public FlameRDD sample(double f) throws Exception {
        return null;
    }

    @Override
    public FlamePairRDD groupBy(StringToString lambda) throws Exception {
        return null;
    }

    @Override
    public FlameRDD filter(StringToBoolean lambda) throws Exception {
        String op = "/rdd/filter";
        String outputTable = context.invokeOperation(tableName, op, Serializer.objectToByteArray(lambda));
        if (outputTable == null) {
            return null;
        }
        return new FlameRDDImpl(outputTable, context);
    }

    @Override
    public FlameRDD mapPartitions(IteratorToIterator lambda) throws Exception {
        return null;
    }
}
