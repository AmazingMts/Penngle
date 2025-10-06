package cis5550.kvs;

import cis5550.tools.Logger;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Enumeration;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

public class PersistentTable {
    private String name;
    private Path tablePath;
    private RandomAccessFile tableFile;
    private long newRowStartPosition;
    private ConcurrentHashMap<String, Long> rowIndexMap;
    private BufferedOutputStream bops;
    private final PersistentTablePool tablePool;
    private final ReentrantLock fileLock = new ReentrantLock();

    private final byte[] ROW_SEPARATOR = "\n".getBytes(StandardCharsets.UTF_8);
    private final Logger logger = Logger.getLogger(PersistentTable.class);

    public PersistentTable(String path) throws FileNotFoundException {
        this.tablePath = Path.of(path);
        this.tableFile = new RandomAccessFile(path, "rw");
        this.newRowStartPosition = 0;
        this.rowIndexMap = new ConcurrentHashMap<>();
        this.bops = new BufferedOutputStream(new FileOutputStream(path));
        this.tablePool = new PersistentTablePool(path, 1000);
    }

    public synchronized void putRow(String key, Row row) throws IOException {
        RandomAccessFile raf = null;
        fileLock.lock();
        try {
            raf = tablePool.acquireForWrite(tablePath.getFileName().toString());
            long rowPosition = newRowStartPosition;
            raf.seek(newRowStartPosition);
            byte[] rowData = row.toByteArray();
            raf.write(rowData);
            raf.write(ROW_SEPARATOR);
            newRowStartPosition = raf.getFilePointer();
            rowIndexMap.put(key, rowPosition);
        } finally {
            if (raf != null) {
                tablePool.release(tablePath.getFileName().toString(), raf, false);
            }
            fileLock.unlock();
        }
    }

    public synchronized void putRows(List<Row> rowList) throws IOException {
        fileLock.lock();
        RandomAccessFile raf = null;
        try {
            raf = tablePool.acquireForWrite(tablePath.getFileName().toString());
            for (Row row : rowList) {
                long rowPosition = newRowStartPosition;
                byte[] rowData = row.toByteArray();
                raf.seek(newRowStartPosition);
                raf.write(rowData);
                raf.write(ROW_SEPARATOR);
                newRowStartPosition = raf.getFilePointer();
                rowIndexMap.put(row.key(), rowPosition);
            }
        } finally {
            if (raf != null) {
                tablePool.release(tablePath.getFileName().toString(), raf, false);
            }
            fileLock.unlock();
        }
    }

    public synchronized Row getRow(String key) throws IOException {
        Long position = rowIndexMap.get(key);
        if (position == null) {
            return null;
        }
        RandomAccessFile raf = null;
        try {
            raf = tablePool.acquireForRead(tablePath.getFileName().toString());
            raf.seek(position);
            return Row.readFrom(raf);
        } catch (Exception e) {
            logger.error("Error reading row", e);
            throw new IOException("Error reading row", e);
        } finally {
            if (raf != null) {
                tablePool.release(tablePath.getFileName().toString(), raf, true);
            }
        }
    }

    public synchronized void recoverTable() throws Exception {
        while (this.tableFile.getFilePointer() < this.tableFile.length()) {
            long index = this.tableFile.getFilePointer();
            Row row = Row.readFrom(this.tableFile);
            assert row != null;
            this.rowIndexMap.put(row.key(), index);
        }
    }

    public int size() {return this.rowIndexMap.size();}
    public Enumeration<String> getKeys() {return this.rowIndexMap.keys();}
    public boolean containsKey(String key) {return this.rowIndexMap.containsKey(key);}

    public synchronized void rename(String oldName, String newName) throws IOException {
        tableFile.close();
        String newFilePath = tablePath.toFile().getAbsolutePath().replace(oldName, newName);
        Path newPath = Path.of(newFilePath);
        Files.move(this.tablePath, newPath);
        this.tablePath = newPath;
        tableFile = new RandomAccessFile(this.tablePath.toFile(), "rw");
    }

    public synchronized void delete() throws IOException {
        tableFile.close();
        Files.delete(tablePath);
    }
}
