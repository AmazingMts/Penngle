package cis5550.flame;

public class UUIDGenerator {
    private long id = 0;

    public synchronized String generateUniqueID() {
        return String.valueOf(id++);
    }
}
