package cis5550.kvs;

import cis5550.tools.KeyEncoder;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class TableHandler {
    private ConcurrentHashMap<String, PersistentTable> persistentTableMap;
    private static ConcurrentHashMap<String, ConcurrentHashMap<String, Row>> inMemoryTableMap = new ConcurrentHashMap<>();
    private final String storageDirectory;

    public TableHandler(String storageDirectory) throws Exception {
        this.storageDirectory = storageDirectory;
        this.persistentTableMap = new ConcurrentHashMap<>();
        this.inMemoryTableMap = new ConcurrentHashMap<>();

        recoverTablesFromFiles(storageDirectory);
    }

    private void recoverTablesFromFiles(String storageDirectory) throws Exception {
        File storageDir = new File(storageDirectory);
        File[] files = storageDir.listFiles();
        if (files != null) {
            for (File file : files) {
                PersistentTable table = new PersistentTable(file.getAbsolutePath());
                table.recoverTable();
                String tableName = file.getName();
                this.persistentTableMap.put(tableName, table);
            }
        }
    }

    public synchronized void createTableIfNecessary(String tableName) throws FileNotFoundException {
        if (tableName.startsWith("pt-")) {
            if (!persistentTableMap.containsKey(tableName)) {
                PersistentTable table = new PersistentTable(storageDirectory + File.separator + tableName);
                persistentTableMap.put(tableName, table);
            }
        } else if (!inMemoryTableMap.containsKey(tableName)) {
            inMemoryTableMap.put(tableName, new ConcurrentHashMap<>());
        }
    }

    public void putRow(String tableName, Row row) throws IOException {
        createTableIfNecessary(tableName);
        synchronized (this) {
            if (tableName.startsWith("pt-")) {
                PersistentTable table = persistentTableMap.get(tableName);
                if (table.containsKey(row.key())) {
                    Row existingRow = table.getRow(row.key());
                    for (String col : row.columns()) {
                        existingRow.put(col, row.get(col));
                    }
                    table.putRow(existingRow.key(), existingRow);
                } else {
                    table.putRow(row.key(), row);
                }
            } else {
                inMemoryTableMap.get(tableName).put(row.key(), row);
            }
        }
    }

    public void putRows(String tableName, List<Row> rows) throws IOException {
        createTableIfNecessary(tableName);
        synchronized (this) {
            if (tableName.startsWith("pt-")) {
                PersistentTable table = persistentTableMap.get(tableName);
                for (Row row : rows) {
                    if (table.containsKey(row.key())) {
                        Row existingRow = table.getRow(row.key());
                        for (String col : existingRow.columns()) {
                            row.put(col, existingRow.get(col));
                        }
                    }
                }
                table.putRows(rows);
            } else {
                ConcurrentHashMap<String, Row> table = inMemoryTableMap.get(tableName);
                for (Row row : rows) {
                    table.put(row.key(), row);
                }
            }
        }
    }

    public synchronized Row getRow(String tableName, String key) throws IOException {
        if (tableName.startsWith("pt-")) {
            if (persistentTableMap.containsKey(tableName)) {
                PersistentTable table = persistentTableMap.get(tableName);
                if (table.containsKey(key)) {
                    return table.getRow(key);
                } else {
                    return null;
                }
            } else {
                return null;
            }
        } else {
            if (inMemoryTableMap.containsKey(tableName)) {
                return inMemoryTableMap.get(tableName).get(key);
            } else {
                return null;
            }
        }
    }

    public synchronized void rename(String oldTableName, String newTableName) throws IOException {
        if (oldTableName.startsWith("pt-")) {
            if (persistentTableMap.containsKey(oldTableName)) {
                PersistentTable table = persistentTableMap.get(oldTableName);
                table.rename(oldTableName, newTableName);
                this.persistentTableMap.put(newTableName, persistentTableMap.remove(oldTableName));
            }
        } else {
            ConcurrentHashMap<String, Row> table = inMemoryTableMap.get(oldTableName);
            for (Map.Entry<String, Row> entry : table.entrySet()) {
                putRow(newTableName, entry.getValue());
            }
        }
    }

    public synchronized void delete(String tableName) throws IOException {
        if (tableName.startsWith("pt-")) {
            PersistentTable table = this.persistentTableMap.get(tableName);
            table.delete();
            this.persistentTableMap.remove(tableName);
        } else {
            inMemoryTableMap.remove(tableName);
        }
    }

    public synchronized boolean containsKey(String tableName) {
        if (tableName.startsWith("pt-")) {
            return persistentTableMap.containsKey(tableName);
        } else {
            return inMemoryTableMap.containsKey(tableName);
        }
    }

    public synchronized PersistentTable getPersistentTable(String tableName) throws FileNotFoundException {
        createTableIfNecessary(tableName);
        return persistentTableMap.get(tableName);
    }

    public synchronized ConcurrentHashMap<String, Row> getInMemoryTable(String tableName) throws FileNotFoundException {
        createTableIfNecessary(tableName);
        return inMemoryTableMap.get(tableName);
    }

    public synchronized ConcurrentHashMap<String, ConcurrentHashMap<String, Row>> getInMemoryTableMap() {
        return inMemoryTableMap;
    }
}
