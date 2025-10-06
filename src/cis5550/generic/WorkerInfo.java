package cis5550.generic;

public class WorkerInfo {
    private String id;
    private String ip;
    private String port;
    private long lastPing;

    public WorkerInfo(String id, String ip, String port) {
        this.id = id;
        this.ip = ip;
        this.port = port;
        this.lastPing = System.currentTimeMillis();
    }

    public String getId() {
        return id;
    }

    public String getIp() {
        return ip;
    }

    public String getPort() {
        return port;
    }

    public long getLastPing() {
        return lastPing;
    }

    public void setLastPing(long lastPing) {
        this.lastPing = lastPing;
    }
}
