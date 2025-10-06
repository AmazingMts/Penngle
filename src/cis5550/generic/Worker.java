package cis5550.generic;

import cis5550.tools.Logger;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.*;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

public class Worker {

    private static final Logger logger = Logger.getLogger(Worker.class);

    protected static List<String> workers = new CopyOnWriteArrayList<>();

    public static void startPingThread(String coordinatorIP, int coordinatorPort, String id, int portNumber) {
        (new Thread(() -> {
//            logger.info(" Ping thread started");
            while(true) {
                try {
                    // Build the URL for the /ping request
                    String pingUrl = "http://" + coordinatorIP + ":" + coordinatorPort + "/ping?id=" + id + "&port=" + portNumber;
                    //"http://" + var0 + "/ping?id=" + var1 + "&port=" + var2
                    // Send the HTTP GET request to the coordinator
                    URI pinUri = new URI(pingUrl);
                    URL url = pinUri.toURL();
                    url.getContent();

//                    logger.info(" Sent ping request with: " + url);

                    Thread.sleep(5000);
                } catch (Exception e) {
                    logger.info(" Error: " + e);
                }
            }

        })).start();
    }

}
