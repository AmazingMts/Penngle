package cis5550.kvs;

import static cis5550.webserver.Server.*;

public class Coordinator extends cis5550.generic.Coordinator {
    public static void main(String[] args) {
        int portNumber = 0;
        try {
            portNumber = Integer.parseInt(args[0]);
        } catch (NumberFormatException e) {
            System.exit(-1);
        }

        port(portNumber);

        registerRoutes();

        get("/", (req, res) -> {
            res.type("text/html");
            return "<html><body><h1>KVS Coordinator</h1>" + workerTable() + "</body></html>";
        });

    }
}
