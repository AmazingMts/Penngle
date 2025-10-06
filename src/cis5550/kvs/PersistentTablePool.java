package cis5550.kvs;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;
import cis5550.tools.Logger;

public class PersistentTablePool {
    private static final Logger logger = Logger.getLogger(PersistentTablePool.class);

    // Inner class to manage file handles for each table
    private static class TableFilePool {
        private final List<RandomAccessFile> readPool;
        private final List<RandomAccessFile> writePool;
        private final ReentrantLock readLock = new ReentrantLock();
        private final ReentrantLock writeLock = new ReentrantLock();
        private final String filePath;
        private final int maxPoolSize;

        public TableFilePool(String filePath, int maxPoolSize) {
            this.filePath = filePath;
            this.maxPoolSize = maxPoolSize;
            this.readPool = new ArrayList<>(maxPoolSize);
            this.writePool = new ArrayList<>(maxPoolSize);
            initializePools();
        }

        private void initializePools() {
            try {
                // Initialize read pool
                for (int i = 0; i < maxPoolSize; i++) {
                    readPool.add(new RandomAccessFile(filePath, "r"));
                }
                // Initialize write pool
                for (int i = 0; i < maxPoolSize/2; i++) {
                    writePool.add(new RandomAccessFile(filePath, "rw"));
                }
            } catch (IOException e) {
                logger.error("Error initializing file pools for: " + filePath, e);
            }
        }

        public RandomAccessFile acquireForRead() {
            readLock.lock();
            try {
                if (!readPool.isEmpty()) {
                    return readPool.remove(readPool.size() - 1);
                }
                return new RandomAccessFile(filePath, "r");
            } catch (IOException e) {
                logger.error("Error acquiring read file handle for: " + filePath, e);
                return null;
            } finally {
                readLock.unlock();
            }
        }

        public RandomAccessFile acquireForWrite() {
            writeLock.lock();
            try {
                if (!writePool.isEmpty()) {
                    return writePool.remove(writePool.size() - 1);
                }
                return new RandomAccessFile(filePath, "rw");
            } catch (IOException e) {
                logger.error("Error acquiring write file handle for: " + filePath, e);
                return null;
            } finally {
                writeLock.unlock();
            }
        }

        public void releaseRead(RandomAccessFile raf) {
            if (raf == null) return;
            readLock.lock();
            try {
                if (readPool.size() < maxPoolSize) {
                    readPool.add(raf);
                } else {
                    try {
                        raf.close();
                    } catch (IOException e) {
                        logger.error("Error closing read file handle", e);
                    }
                }
            } finally {
                readLock.unlock();
            }
        }

        public void releaseWrite(RandomAccessFile raf) {
            if (raf == null) return;
            writeLock.lock();
            try {
                if (writePool.size() < maxPoolSize/2) {
                    writePool.add(raf);
                } else {
                    try {
                        raf.close();
                    } catch (IOException e) {
                        logger.error("Error closing write file handle", e);
                    }
                }
            } finally {
                writeLock.unlock();
            }
        }

        public void close() {
            readLock.lock();
            writeLock.lock();
            try {
                // Close all read handles
                for (RandomAccessFile raf : readPool) {
                    try {
                        raf.close();
                    } catch (IOException e) {
                        logger.error("Error closing read file handle", e);
                    }
                }
                readPool.clear();

                // Close all write handles
                for (RandomAccessFile raf : writePool) {
                    try {
                        raf.close();
                    } catch (IOException e) {
                        logger.error("Error closing write file handle", e);
                    }
                }
                writePool.clear();
            } finally {
                writeLock.unlock();
                readLock.unlock();
            }
        }
    }

    private final ConcurrentHashMap<String, TableFilePool> tablePools;
    private final String storageDirectory;
    private final int maxPoolSizePerTable;

    public PersistentTablePool(String storageDirectory, int maxPoolSizePerTable) {
        this.storageDirectory = storageDirectory;
        this.maxPoolSizePerTable = maxPoolSizePerTable;
        this.tablePools = new ConcurrentHashMap<>();
    }

    private TableFilePool getOrCreatePool(String tableName) {
        return tablePools.computeIfAbsent(tableName, name -> {
            String filePath = Path.of(storageDirectory).toString();
            return new TableFilePool(filePath, maxPoolSizePerTable);
        });
    }

    public RandomAccessFile acquireForRead(String tableName) {
        return getOrCreatePool(tableName).acquireForRead();
    }

    public RandomAccessFile acquireForWrite(String tableName) {
        return getOrCreatePool(tableName).acquireForWrite();
    }

    public void release(String tableName, RandomAccessFile raf, boolean isRead) {
        TableFilePool pool = tablePools.get(tableName);
        if (pool != null) {
            if (isRead) {
                pool.releaseRead(raf);
            } else {
                pool.releaseWrite(raf);
            }
        }
    }

    public void removeTable(String tableName) {
        TableFilePool pool = tablePools.remove(tableName);
        if (pool != null) {
            pool.close();
        }
    }

    public void closeAll() {
        for (TableFilePool pool : tablePools.values()) {
            pool.close();
        }
        tablePools.clear();
    }

    // Optional: Add metrics methods
    public int getActiveReadHandles(String tableName) {
        TableFilePool pool = tablePools.get(tableName);
        return pool != null ? pool.maxPoolSize - pool.readPool.size() : 0;
    }

    public int getActiveWriteHandles(String tableName) {
        TableFilePool pool = tablePools.get(tableName);
        return pool != null ? pool.maxPoolSize/2 - pool.writePool.size() : 0;
    }
}