package cis5550.webserver;

import java.util.*;
import java.net.*;
import java.nio.charset.*;
import java.security.SecureRandom;

// Provided as part of the framework code

class RequestImpl implements Request {
  String method;
  String url;
  String protocol;
  InetSocketAddress remoteAddr;
  Map<String,String> headers;
  Map<String,String> queryParams;
  Map<String,String> params;
  byte bodyRaw[];
  String newSessionID=null;
  Server server;

  RequestImpl(String methodArg, String urlArg, String protocolArg, Map<String,String> headersArg, Map<String,String> queryParamsArg, Map<String,String> paramsArg, InetSocketAddress remoteAddrArg, byte bodyRawArg[], Server serverArg) {
    method = methodArg;
    url = urlArg;
    remoteAddr = remoteAddrArg;
    protocol = protocolArg;
    headers = headersArg;
    queryParams = queryParamsArg;
    params = paramsArg;
    bodyRaw = bodyRawArg;
    server = serverArg;
  }

  public String requestMethod() {
  	return method;
  }
  public void setParams(Map<String,String> paramsArg) {
    params = paramsArg;
  }
  public int port() {
  	return remoteAddr.getPort();
  }
  public String url() {
  	return url;
  }
  public String protocol() {
  	return protocol;
  }
  public String contentType() {
  	return headers.get("content-type");
  }
  public String ip() {
  	return remoteAddr.getAddress().getHostAddress();
  }
  public String body() {
    return new String(bodyRaw, StandardCharsets.UTF_8);
  }
  public byte[] bodyAsBytes() {
  	return bodyRaw;
  }
  public int contentLength() {
  	return bodyRaw.length;
  }
  public String headers(String name) {
  	return headers.get(name.toLowerCase());
  }
  public Set<String> headers() {
  	return headers.keySet();
  }
  public String queryParams(String param) {
  	return queryParams.get(param);
  }
  public Set<String> queryParams() {
  	return queryParams.keySet();
  }
  public String params(String param) {
    return params.get(param);
  }
  public String getNewSessionID(){
    return newSessionID;
  }
  public Map<String,String> params() {
    return params;
  }

  public Session session(){ 
    if(headers.get("cookie")!=null&&headers.get("cookie").contains("SessionID="))
    {
      String cookie = headers.get("cookie");
      String sID = cookie.substring(cookie.indexOf("SessionID=")+10).split(";")[0];
      SessionImpl sess = server.getSessionRecord().get(sID);
      if(sess!=null)
      {
        long currTime=System.currentTimeMillis();
        if(currTime-sess.lastAccessedTime()<=sess.getTTL())
        {
          sess.updateTime(currTime);
          return server.getSessionRecord().get(sID);
        }
        else
        {
          server.getSessionRecord().remove(sID);
        }
      }
    }
    String nID = generateSessionId();
    newSessionID = nID;
    SessionImpl newSession = new SessionImpl(System.currentTimeMillis(),nID);
    server.getSessionRecord().put(nID, newSession);

    return newSession; 
  }

  public static String generateSessionId() {
      SecureRandom random = new SecureRandom();
      byte[] bytes=new byte[15]; // 15*8=120
      random.nextBytes(bytes);
      String id=new String();
      for (byte b : bytes) {
        id+=String.format("%02x", b);
      }
      return id;
  }

}
