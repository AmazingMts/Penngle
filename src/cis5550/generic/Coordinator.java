package cis5550.generic;

import cis5550.tools.Logger;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import static cis5550.webserver.Server.*;

public class Coordinator {

    private static final Logger logger = Logger.getLogger(Coordinator.class);

    private static ConcurrentHashMap<String, WorkerInfo> workerMap = new ConcurrentHashMap<>();

    public static List<String> getWorkers() {
        List<String> workerList = new ArrayList<>();
        removeExpiredWorkers();
        for (WorkerInfo workerInfo : workerMap.values()) {
//            logger.info("Valid worker: " + workerInfo.getId());
            StringBuilder sb = new StringBuilder();
            sb.append(workerInfo.getIp()).append(":").append(workerInfo.getPort());
            workerList.add(sb.toString());
        }
//        logger.info("Current workers: " + workerList);
        return workerList;
    }

    // Remove workers who haven't pinged in the last 15 seconds
    public static void removeExpiredWorkers() {
//        logger.info("---Removing expired workers---");
        Iterator<Map.Entry<String, WorkerInfo>> iterator = workerMap.entrySet().iterator();

        while (iterator.hasNext()) {
            Map.Entry<String, WorkerInfo> entry = iterator.next();
            WorkerInfo worker = entry.getValue();

//            logger.info(" Removing worker: " + worker.getId());

            // Check if the worker's last ping was more than 15 seconds ago
            if (System.currentTimeMillis() - worker.getLastPing() >= 15 * 1000) {
                iterator.remove();  // Remove the worker from the list
            }
        }
    }

    public static String workerTable() {
        StringBuilder html = new StringBuilder();

        html.append("<html><body><h1>Worker List</h1>");
        html.append("<table border='1'>");
        html.append("<tr><th>ID</th><th>IP</th><th>Port</th><th>Metrics</th></tr>");

        removeExpiredWorkers();

        for (WorkerInfo worker : workerMap.values()) {
            html.append("<tr>");
            html.append("<td>").append(worker.getId()).append("</td>");

            String baseUrl = "http://" + worker.getIp() + ":" + worker.getPort();

            html.append("<td><a href='").append(baseUrl).append("'>")
                    .append(worker.getIp()).append("</a></td>");

            html.append("<td>").append(worker.getPort()).append("</td>");

            html.append("<td><a href='").append(baseUrl).append("/metrics")
                    .append("'>View Metrics</a></td>");

            html.append("</tr>");
        }

        html.append("</table></body></html>");
        return html.toString();
    }

    public static void registerRoutes() {
        get("/ping", (req, res) -> {
            String id = req.queryParams("id");
            String port = req.queryParams("port");

            if (id == null || port == null) {
                res.status(400, "Bad Request");
                return "Error: Missing id or port.";
            }

            String ip = req.ip();  // Get the IP address of the worker
            WorkerInfo worker = workerMap.get(id);

            if (worker == null) {
                // If the worker is new, add it to the map
//                logger.info(" Adding new worker: " + id);
                worker = new WorkerInfo(id, ip, port);
                workerMap.put(id, worker);
            } else {
                // Update the existing worker's IP, port, and last ping time
//                logger.info(" Updating worker: " + id);
                worker.setLastPing(System.currentTimeMillis());
            }

            res.status(200, "OK");
            res.body("OK");
            return "OK";
        });

        get("/workers", (req, res) -> {
            List<String> workers = getWorkers();
//            logger.info(" Current number of workers: " + workers.size());

            StringBuilder result = new StringBuilder();
            result.append(workers.size()).append("\n");

            for (WorkerInfo worker : workerMap.values()) {
                result.append(worker.getId()).append(",").append(worker.getIp()).append(":").append(worker.getPort()).append("\n");
            }
            res.status(200, "OK");
            res.body(result.toString());
            return result.toString();
        });

    }

}


