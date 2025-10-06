package cis5550.flame;

import cis5550.kvs.*;
import cis5550.tools.Logger;
import cis5550.tools.Serializer;

import java.util.List;
import java.util.Iterator;
import java.util.ArrayList;

public class FlamePairRDDImpl implements FlamePairRDD {

    private static final Logger logger = Logger.getLogger(FlamePairRDDImpl.class);

    private String tableName;
    private FlameContextImpl context;

    public FlamePairRDDImpl(String tableName, FlameContextImpl context) {
        this.tableName = tableName;
        this.context = context;
    }

    public String getTableName() {
        return tableName;
    }

    @Override
    public List<FlamePair> collect() throws Exception {
        KVSClient kvs = context.getKVS();
        List<FlamePair> result = new ArrayList<>();

        Iterator<Row> iter = kvs.scan(tableName);
        while (iter.hasNext()) {
            Row row = iter.next();
            String key = row.key();
            for (String column : row.columns()) {
                String value = row.get(column);
                result.add(new FlamePair(key, value));
            }
        }
        return result;
    }

    @Override
    public FlamePairRDD foldByKey(String zeroElement, TwoStringsToString lambda) throws Exception {
        FlamePair extraParam = new FlamePair("zeroElement", zeroElement);

//        logger.info("---foldByKey--- invoked, zeroElement: " + zeroElement);

        String op = "/pairrdd/foldByKey";
        String outputTable = context.invokeOperation(tableName, op, Serializer.objectToByteArray(lambda), extraParam);
        if (outputTable == null) {
            return null;
        } else {
            return new FlamePairRDDImpl(outputTable, context);
        }
    }

    @Override
    public void saveAsTable(String tableNameArg) throws Exception {
        context.getKVS().rename(tableName, tableNameArg);
        tableName = tableNameArg;
    }

    @Override
    public FlameRDD flatMap(PairToStringIterable lambda) throws Exception {
        String op = "/pairrdd/flatMap";
        String outputTable = context.invokeOperation(tableName, op, Serializer.objectToByteArray(lambda));
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
    public FlamePairRDD flatMapToPair(PairToPairIterable lambda) throws Exception {
        String op = "/pairrdd/flatMapToPair";
        String outputTable = context.invokeOperation(tableName, op, Serializer.objectToByteArray(lambda));
        if (outputTable == null) {
            return null;
        } else {
            return new FlamePairRDDImpl(outputTable, context);
        }
    }

    @Override
    public FlamePairRDD join(FlamePairRDD other) throws Exception {
        if (!(other instanceof FlamePairRDDImpl otherTable)) {
            throw new IllegalArgumentException("Argument must be of type FlameRDDImpl");
        }
        FlamePair extraParam = new FlamePair("otherTable", otherTable.getTableName());
        String op = "/pairrdd/join";
        String outputTable = context.invokeOperation(tableName, op, null, extraParam);
        if (outputTable == null) {
            return null;
        } else {
            return new FlamePairRDDImpl(outputTable, context);
        }
    }

    @Override
    public FlamePairRDD cogroup(FlamePairRDD other) throws Exception {
        return null;
    }
}
