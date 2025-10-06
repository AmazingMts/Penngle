package cis5550.webserver;
import java.util.HashMap;
import java.util.Map;

public class SessionImpl implements Session{
    long mCreationTime;
    long mLastAccessTime;
    String mID;
    int mMaxActiveInterval;
    Map<String,Object> mKeyValues=new HashMap<>();
    SessionImpl(long currTime,String ID)
    {
        mCreationTime=currTime;
        mLastAccessTime=currTime;
        mID=ID;
        mMaxActiveInterval=300;
    }
    public String id() { return mID; }
    public long creationTime(){ return mCreationTime; }
    public void updateTime(long time){ mLastAccessTime = time; }
    public long getTTL() { return (long)mMaxActiveInterval*1000; }
    public long lastAccessedTime(){ return mLastAccessTime; }
    public void maxActiveInterval(int seconds){ mMaxActiveInterval=seconds; }
    public void invalidate(){}
    public Object attribute(String name){ return mKeyValues.get(name); }
    public void attribute(String name, Object value){ mKeyValues.put(name, value); }
}
