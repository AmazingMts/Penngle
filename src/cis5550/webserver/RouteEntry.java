package cis5550.webserver;

import cis5550.tools.Logger;

import java.util.Map;

public class RouteEntry {

    private static final Logger logger = Logger.getLogger(RouteEntry.class);

    private String method;

    private String path;

    private Route handler;

    private Map<String, String> params;

    public RouteEntry(String method, String path, Route handler) {
        this.method = method;
        this.path = path;
        this.handler = handler;
    }

    public String getMethod() {
        return method;
    }

    public void setMethod(String method) {
        this.method = method;
    }

    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }

    public Route getHandler() {
        return handler;
    }

    public void setHandler(Route handler) {
        this.handler = handler;
    }

    public String toString() {
        return "Method: " + method + ", Path: " + path;
    }

    public Map<String, String> getParams() {
        return params;
    }

    public void setParams(Map<String, String> params) {
        this.params = params;
    }

}
