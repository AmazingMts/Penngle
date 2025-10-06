package cis5550.webserver;

import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ResponseImpl implements Response {
    byte bodyRaw[] = new byte[0];
    Map<String,List<String>> headers = new HashMap<>();
    int statusC=200;
    String statusStr="OK";
    Socket socket;
    boolean writeCalled = false;
    String redirectURL = null;
    ResponseImpl(Socket s, int code){ 
        socket=s;
        statusC=code;
        // header("content-type", "text/html");
    }
    public int getStatusCode() { return statusC; }
    public byte[] getBodyRaw() { return bodyRaw; }
    public String getStatusString() { return statusStr; }
    public Map<String,List<String>> getHeaders() { return headers; }
    public boolean getWriteCalled() { return writeCalled; }
    public void body(String body)
    {
        if(writeCalled) return;
        bodyRaw=body.getBytes();
    }
    public void bodyAsBytes(byte bodyArg[])
    {
        if(writeCalled) return;
        bodyRaw=bodyArg;
    }
    public void header(String name, String value)
    {
        if(writeCalled) return;
        headers.putIfAbsent(name, new ArrayList<>());
        headers.get(name).add(value);
    }
    public void type(String contentType)
    {
        header("Content-Type",contentType);
    }
    public void status(int statusCode, String reasonPhrase)
    {
        statusC=statusCode;
        statusStr=reasonPhrase;
    }
    public void write(byte[] b)
    {
        try {
            OutputStream output = socket.getOutputStream();
            if(!writeCalled)
            {
                writeCalled=true;
                String response = "HTTP/1.1 "+getStatusCode()+" "+getStatusString()+"\r\n";
                for (Map.Entry<String, List<String>> mEntry : getHeaders().entrySet()) {
                    String header = mEntry.getKey();
                    List<String> contents = mEntry.getValue();
                    for (String content : contents) {
                        response += header + ": " + content + "\r\n";
                    }
                }
                response += "Connection: close\r\n\r\n";
                output.write(response.getBytes(StandardCharsets.US_ASCII));
                //output.flush();
            }
            output.write(b);
            output.flush();
        }catch(IOException e)
        {
            e.printStackTrace();
        }
    }
    public void redirect(String url, int responseCode)
    {
        redirectURL=url;
        statusC=responseCode;
    }
    public void halt(int statusCode, String reasonPhrase)
    {
        
    }
}
