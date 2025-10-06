package cis5550.webserver;

import cis5550.tools.Logger;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.KeyStore;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TimeZone;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLServerSocketFactory;

public class Server implements Runnable {
    public void run(){
		//Uncomment the following comments to pass the hw3 test since it needs to run on ports
        boolean checkKeyExisted = new File("./keystore.jks").exists();
        if (checkKeyExisted && secPort > 0){
            ExecutorService poolTLS = Executors.newFixedThreadPool(NUM_WORKERS);
            new Thread(() -> startTLSServer(poolTLS)).start();
        }else{
            ExecutorService poolNonTLS = Executors.newFixedThreadPool(NUM_WORKERS);
            new Thread(() -> startNonTLSServer(poolNonTLS)).start();
        }
        SessionExpirer sessionExpirer = new SessionExpirer(sessionRecord);
        sessionExpirer.start();
    }

    private ServerSocket createSSLServerSocket(int port) throws Exception {
        String pwd = "secret";
        KeyStore keyStore = KeyStore.getInstance("JKS");
        keyStore.load(new FileInputStream("keystore.jks"), pwd.toCharArray());
        KeyManagerFactory keyManagerFactory = KeyManagerFactory.getInstance("SunX509");
        keyManagerFactory.init(keyStore, pwd.toCharArray());
        SSLContext sslContext = SSLContext.getInstance("TLS");
        sslContext.init(keyManagerFactory.getKeyManagers(), null, null);
        SSLServerSocketFactory factory = sslContext.getServerSocketFactory();
        return factory.createServerSocket(port);
    }

    private void startTLSServer(ExecutorService pool) {
        try (ServerSocket serverSocketTLS = createSSLServerSocket(secPort)) {
            System.out.println("SSL Server is listening on port: " + secPort);
            while (true) {
                Socket socket = serverSocketTLS.accept();
                pool.execute(new RequestHandler(socket));
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void startNonTLSServer(ExecutorService pool) {
        try (ServerSocket serverSocket = new ServerSocket(port)) {
            System.out.println("Server is listening on port: " + port);
            while (true) {
                Socket socket = serverSocket.accept();
                pool.execute(new RequestHandler(socket));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    static class WorkerThread extends Thread {
        private BlockingQueue<Runnable> taskQueue;

        public WorkerThread(BlockingQueue<Runnable> queue) {
            this.taskQueue = queue;
        }

        @Override
        public void run() {
            while (true) {
                try {
                    Runnable task = taskQueue.take();
                    task.run();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    private static class RequestHandler implements Runnable {
        private final Socket socket;

        public RequestHandler(Socket socket) {
            this.socket = socket;
        }

        @Override
        public void run() {
            int prevPrevByte = -1;
            int prevByte = -1;
            int currentByte = -1;
            while(!socket.isClosed())
            {   
                //System.out.println("Socket is opened");
                try {
                    InputStream inputS = socket.getInputStream();
                    ByteArrayOutputStream byteBuf = new ByteArrayOutputStream();
                    List<String> headerLines = new ArrayList<>();
                    while((currentByte=inputS.read())!=-1)
                    {
                        byteBuf.write(currentByte);
                        if(prevByte=='\r'&&currentByte=='\n')
                        {
                            if(byteBuf.toString().length()>2)
                            {
                                String line = byteBuf.toString().substring(0,byteBuf.toString().length()-2);
                                headerLines.add(line);
                            }
                            byteBuf.reset();
                        }
                        if(prevPrevByte=='\r'&&prevByte=='\n'&&currentByte=='\r')
                        {
                            currentByte=inputS.read();
                            if(currentByte==-1)
                            {
                                socket.close();
                                break;
                            }
                            if(currentByte=='\n')
                                break;
                        }
                        prevPrevByte=prevByte;
                        prevByte=currentByte;
                    }
                    if(headerLines.size()>0)
                    {
                        final String[] requestArray =  headerLines.get(0).split("\\s+");
                        OutputStream output = socket.getOutputStream();
                        PrintWriter writer = new PrintWriter(output, true);
                        boolean requestValid=true;
                        boolean foundHost=false;
                        long contentLength=0;
                        Date imsDate=null;
                        int contentStart=-1,contentEnd=-1;
                        Map<String,String> headers=new HashMap<>();
                        String requestHost = "";
                        if(requestArray.length!=3)
                        {
                            requestValid=false;
                        }
                        if(requestValid)
                        {
                            for(int i=1;i<headerLines.size();i++)
                            {
                                int pos=headerLines.get(i).indexOf(':');
                                
                                //String[] headerArray = headerLines.get(i).split("\\s+");
                                if(pos!=-1)
                                {
                                    String header = headerLines.get(i).substring(0, pos).toLowerCase();
                                    String content = headerLines.get(i).substring(pos+2, headerLines.get(i).length());
                                    //System.out.println("Header: "+header+" Content: "+content);
                                    headers.put(header, content);
                                    if(header.equals("host"))
                                    {
                                        foundHost=true;
                                        int pos1=content.indexOf(':');
                                        if(pos1==-1)
                                            requestHost=content;
                                        else
                                            requestHost=content.substring(0, pos1);
                                    }
                                    if(header.equals("content-length"))
                                    {
                                        contentLength=Long.parseLong(content);
                                    }
                                    if(header.equals("if-modified-since"))
                                    {
                                        SimpleDateFormat format = new SimpleDateFormat("EEE, dd MMM yyyy HH:mm:ss zzz", Locale.US);
                                        format.setTimeZone(TimeZone.getTimeZone("GMT"));
                                        try{
                                            imsDate=format.parse(content);
                                        }
                                        catch(ParseException e)
                                        {
                                            imsDate=null;
                                        }
                                    }
                                    if(header.equals("range")&&requestArray[0].equals("GET"))
                                    {
                                        String[] startEnd = content.replace("bytes=", "").split("-");
                                        if(startEnd[0].isEmpty())
                                        {
                                            contentEnd=Integer.parseInt(startEnd[1]);
                                        }
                                        else
                                        {
                                            contentStart=Integer.parseInt(startEnd[0]);
                                            contentEnd=startEnd.length>1&&(!startEnd[1].isEmpty())?Integer.parseInt(startEnd[1]):-1;
                                        }
                                    }
                                }
                            }
                            if(!foundHost) requestValid=false;
                        }
                        if(requestValid)
                        {
                            //System.out.println("request is valid");
                            ByteArrayOutputStream bodyBuffer=new ByteArrayOutputStream();
                            if(contentLength>0)
                            {
                                for(long i=0;i<contentLength;i++)
                                    bodyBuffer.write(inputS.read());
                            }
                            if(!requestArray[2].equals("HTTP/1.1"))
                            {
                                String response = "HTTP/1.1 505 HTTP Version Not Supported\r\n";
                                writer.print(response+defaultHeaders);
//                                logger.info("Response: "+response+defaultHeaders);
//                                System.out.println("Response: "+response+defaultHeaders);
                                writer.flush();
                                return;
                            }
                            
                            Map<String, Map<String, Route>> hostRoutes=routes.get(requestHost);
                            if(hostRoutes==null)
                            {
                                hostRoutes=routes.get("");
                            }
                            boolean isDynamic=handleDynamic(socket,writer,hostRoutes,requestArray[0],requestArray[1],requestArray[2],bodyBuffer,headers,200);
                            if(!isDynamic)
                            {
                                if(requestArray[0].equals("GET")||requestArray[0].equals("HEAD"))
                                {
                                    boolean isTxt = requestArray[1].toLowerCase().endsWith(".txt");
                                    boolean isHtml = requestArray[1].toLowerCase().endsWith(".html");
                                    boolean isJpeg = requestArray[1].toLowerCase().endsWith(".jpeg")||requestArray[1].toLowerCase().endsWith(".jpg");
                                    String path=cwd+requestArray[1];
                                    boolean isForbidden = path.contains("..");
//                                    logger.info("Requested file path: "+path);
                                    if(!isForbidden)
                                    {
                                        File file = new File(path);
                                        if(file.exists())
                                        {
                                            boolean needContent=true;
                                            if(imsDate!=null)
                                            {
                                                long actualLastModified = file.lastModified();
                                                Date almDate = new Date(actualLastModified);
                                                if(imsDate.after(almDate))
                                                    needContent=false;
                                            }
//                                            System.out.println("Need Content: " + needContent);
                                            if(needContent)
                                            {
                                                try {
                                                    byte[] fileContentBytes = Files.readAllBytes(Paths.get(path));
                                                    byte[] slicedContentBytes = null;
                                                    boolean useRange = contentStart!=-1||contentEnd!=-1;
                                                    if(useRange)
                                                    {
                                                        contentStart = contentStart==-1?0:contentStart;
                                                        contentEnd = contentEnd==-1?fileContentBytes.length-1:contentEnd;
                                                        int length = contentEnd - contentStart + 1;
                                                        slicedContentBytes = new byte[length];
                                                        System.arraycopy(fileContentBytes, contentStart, slicedContentBytes, 0, length);
                                                    }
                                                    StringBuilder sb = new StringBuilder();
                                                    if(!useRange)
                                                        sb.append("HTTP/1.1 200 OK\r\nContent-Type: ");
                                                    else
                                                        sb.append("HTTP/1.1 206 Partial Content\r\nContent-Type: ");
                                                                                
                                                    if(isTxt)
                                                        sb.append("text/plain\r\nServer: ");
                                                    else if(isHtml)
                                                        sb.append("text/html\r\nServer: ");
                                                    else if(isJpeg)
                                                        sb.append("image/jpeg\r\nServer: ");
                                                    else
                                                        sb.append("application/octet-stream\r\nServer: ");

                                                    sb.append(serverName);
                                                    sb.append("\r\nContent-Length: ");
                                                    sb.append(fileContentBytes.length);
                                                    if(useRange)
                                                    {
                                                        sb.append("\r\nContent-Range: bytes ");
                                                        sb.append(contentStart);
                                                        sb.append("-");
                                                        sb.append(contentEnd);
                                                        sb.append("/");
                                                        sb.append(fileContentBytes.length);
                                                    }
                                                    sb.append("\r\n\r\n");
                                                    writer.print(sb.toString());
//                                                    System.out.println("sb content:\n" + sb.toString());
                                                    writer.flush();
                                                    if(requestArray[0].equals("GET"))
                                                    {
                                                        if(!useRange)
                                                            output.write(fileContentBytes);
                                                        else
                                                            output.write(slicedContentBytes);
                                                    }
                                                    output.flush();    
                                                } catch (IOException e) {
                                                    e.printStackTrace();
                                                }
                                            }
                                            else
                                            {
                                                String response = "HTTP/1.1 304 Not Modified";
                                                writer.print(response+defaultHeaders);
                                                logger.info("Response: "+response+defaultHeaders);
                                                writer.flush();
                                                System.out.println("Need Content" + response);
                                            }
                                        }
                                        else
                                        {
                                            String response = "HTTP/1.1 404 Not Found";
                                            writer.print(response+defaultHeaders);
                                            logger.info("Response: "+response+defaultHeaders);
                                            writer.flush();
                                            System.out.println("File Not Existed" + response);
                                        }
                                    }
                                    else
                                    {
                                        String response = "HTTP/1.1 403 Forbidden";
                                        writer.print(response+defaultHeaders);
                                        logger.info("Response: "+response+defaultHeaders);
                                        writer.flush();
                                    }
                                }
                                else if(requestArray[0].equals("POST")||requestArray[0].equals("PUT"))
                                {
                                    String response = "HTTP/1.1 405 Not Allowed\r\n";
                                    writer.print(response+defaultHeaders);
                                    logger.info("Response: "+response+defaultHeaders);
                                    writer.flush();
                                }
                                else
                                {
                                String response = "HTTP/1.1 501 Not Implemented\r\n";
                                writer.print(response+defaultHeaders);
                                logger.info("Response: "+response+defaultHeaders);
                                writer.flush();
                                }
                            }
                        }
                        if(!requestValid)
                        {
                            System.out.println("Not valid");
                            String response = "HTTP/1.1 400 Bad Request\r\n";
                            writer.print(response+defaultHeaders);
                            logger.info("Response: "+response+defaultHeaders);
                            writer.flush();
                        }
                    }else{
                        System.out.println("No header lines");
                    }
                    if(currentByte==-1&&!socket.isClosed()) socket.close();
                }
                catch(IOException e)
                {
                    e.printStackTrace();
                    return;
                }
            }
        }
    }

    public static boolean handleDynamic(Socket socket, PrintWriter writer, Map<String, Map<String, Route>> hostRoutes, String method, String url, String protocol, ByteArrayOutputStream bodyBuffer, Map<String,String> headers, int respCode)
    {
        RequestImpl req=null;
        ResponseImpl res=null;
        Route r=null;
        Map<String,String> qParams=new HashMap<>();
        if(url.indexOf("?")!=-1)
        {
            parseQueryParams(url,qParams);
            url=url.substring(0,url.indexOf("?"));
        }
        if(headers.get("content-type")!=null&&headers.get("content-type").equals("application/x-www-form-urlencoded"))
        {
            parseQueryParams(bodyBuffer.toString(),qParams);
        }
        Map<String,Route> entry = hostRoutes.get(url);
        if(entry!=null)
        {
            r=entry.get(method);
        }
        Map<String,String> params=null;
        if(r==null)
        {
            for(Map.Entry<String,Map<String,Route>> mEntry : hostRoutes.entrySet())
            {
                params=matchUrl(url,mEntry.getKey());
                if(params!=null)
                {
                    if(mEntry.getValue().get(method)!=null)
                    {
                        r=mEntry.getValue().get(method);
                        break;
                    }
                }
            }
        }
        

        if(r!=null)
        {
            try{
                req=new RequestImpl(method,url,protocol,headers,qParams,params,(InetSocketAddress)socket.getRemoteSocketAddress(),bodyBuffer.toByteArray(),instance);
                res=new ResponseImpl(socket,respCode);
                String s=(String)r.handle(req,res);
                if(res.redirectURL!=null)
                {
                    handleDynamic(socket,writer,hostRoutes,method,res.redirectURL,protocol,bodyBuffer,headers,res.statusC);
                    return true;
                }
                if(req.getNewSessionID()!=null)
                {
                    res.header("Set-Cookie", "SessionID="+req.getNewSessionID());
                }
                if(!res.writeCalled)
                {
                    String response = "HTTP/1.1 "+res.getStatusCode()+" "+res.getStatusString()+"\r\n";
                    for (Map.Entry<String, List<String>> mEntry : res.getHeaders().entrySet()) {
                        String header = mEntry.getKey();
                        List<String> contents = mEntry.getValue();
                        for (String content : contents) {
                            response += header + ": " + content + "\r\n";
                        }
                    }
                    int dynamicLen = 0;

                    if(s!=null)
                        dynamicLen=s.length();
                    else if(res.getBodyRaw()!=null)
                        dynamicLen=res.getBodyRaw().length;

                    if(!response.contains("Content-Type")){
                        response += "Content-type: text/plain\r\n";
                    }
                    
                    response += "Content-Length: "+ dynamicLen + "\r\n\r\n";

                    if(s!=null)
                        response += s;
                    else
                        response += new String(res.getBodyRaw(), StandardCharsets.UTF_8);
                    //
                    writer.print(response);
                    writer.flush();
//                    System.out.println("response:\n" + response);
                } else{
                    socket.close();
                }
                
            }catch(Exception e)
            {
                e.printStackTrace();
                try{
                    if(res!=null&&res.writeCalled)
                    {
                        socket.close();
                        return true;
                    }
                    String response = "HTTP/1.1 500 Internal Server Error\r\n";
                    writer.print(response+defaultHeaders);
                    writer.flush();
                    socket.close();
                }
                catch(Exception e1)
                {
                    e1.printStackTrace();
                }
            }
            return true;
        }
        return false;
    }

    public static Map<String, String> matchUrl(String url, String pathPattern) 
    {
        String[] urlParts = url.split("/");
        String[] patternParts = pathPattern.split("/");
        if (urlParts.length != patternParts.length) {
            return null;
        }
        Map<String, String> params = new HashMap<>();
        for (int i = 0; i < patternParts.length; i++) {
            if (patternParts[i].startsWith(":")) {
                String paramName = patternParts[i].substring(1);
                params.put(paramName, urlParts[i]);
            } else if (!patternParts[i].equals(urlParts[i])) {
                return null;
            }
        }
        return params;
    }

    public static void parseQueryParams(String url, Map<String, String> queryParams) 
    {
        int queryStart = url.indexOf("?");
        String queryString = url.substring(queryStart + 1);
        int hashIndex = queryString.indexOf("#");
        if (hashIndex != -1) {
            queryString = queryString.substring(0, hashIndex);
        }
        String[] params = queryString.split("&");
        for (String param : params) {
            String[] keyValue = param.split("=", 2);
            String key = URLDecoder.decode(keyValue[0], StandardCharsets.UTF_8);
            String value = keyValue.length > 1 ? URLDecoder.decode(keyValue[1], StandardCharsets.UTF_8) : "";
            //System.out.println(value);
            queryParams.put(key, value);
        }
        return;
    }

    public static void get(String path,Route r)
    {
        if(instance==null)
        {
            instance = new Server();
        }
        if(!running)
        {
            Thread thread = new Thread(instance);
            thread.start();
            running=true;
        }
        addTableEntry("GET",path,r);
    }
    public static void post(String path,Route r)
    {
        if(instance==null)
        {
            instance = new Server();
        }
        if(!running)
        {
            Thread thread = new Thread(instance);
            thread.start();
            running=true;
        }
        addTableEntry("POST",path,r);
    }
    public static void put(String path,Route r)
    {
        if(instance==null)
        {
            instance = new Server();
        }
        if(!running)
        {
            Thread thread = new Thread(instance);
            thread.start();
            running=true;
        }
        addTableEntry("PUT",path,r);
    }
    public static void addTableEntry(String method,String path,Route r)
    {
        routes.putIfAbsent(currHost, new HashMap<>());
        routes.get(currHost).putIfAbsent(path, new HashMap<>());
        routes.get(currHost).get(path).put(method, r);
    }
    public static void port(int num)
    {
        if(instance==null)
        {
            instance = new Server();
        }
        port=num;
    }

    public static void securePort(int num)
    {
        if(instance==null)
        {
            instance = new Server();
        }
        secPort = num;
    }

    public static void host(String hostName)
    {
        currHost = hostName;
    }

    public static class staticFiles {
        public static void location(String s) { 
            if(instance==null)
            {
                instance = new Server();
            }
            if(!running)
            {
                Thread thread = new Thread(instance);
                thread.start();
                running=true;
            }
            cwd=s; 
        }
    }

    public class SessionExpirer {
        private final Map<String, SessionImpl> sessions;
        private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
        public SessionExpirer(Map<String, SessionImpl> sessions) {
            this.sessions = sessions;
        }
        public void start() {
            scheduler.scheduleAtFixedRate(this::expireSessions, 1, 1, TimeUnit.SECONDS);
        }
        private void expireSessions() {
            long now = System.currentTimeMillis();
            sessions.entrySet().removeIf(entry -> (now - entry.getValue().lastAccessedTime()) > entry.getValue().getTTL());
        }
    }

    public Map<String, SessionImpl> getSessionRecord() { return sessionRecord; }
    private static Map<String, SessionImpl> sessionRecord = new ConcurrentHashMap<>();

    private static final Logger logger = Logger.getLogger(Server.class);
    private static final String serverName = "CyberSquad";
    private static final String defaultHeaders = "Content-Type: text/plain\r\nContent-Length: 0\r\n\r\n";
    private static final int NUM_WORKERS  = 100;
    private static String cwd = ".";
    private static String currHost = "";
    private static int port = 80;
    private static int secPort = -1;
    private static Server instance = null;
    private static boolean running = false;
    private static Map<String, Map<String, Map<String, Route>>> routes = new HashMap<>();
}
