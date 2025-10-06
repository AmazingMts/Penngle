package cis5550.jobs;

import cis5550.tools.Hasher;
import cis5550.tools.Logger;
import cis5550.tools.URLParser;
import cis5550.flame.*;
import cis5550.kvs.*;

import java.io.IOException;
import java.lang.reflect.GenericDeclaration;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class PageRank {

    private static final Logger logger = Logger.getLogger(PageRank.class);

    private static final double DECAY_FACTOR = 0.85;
    private static final double DEFAULT_THRESHOLD = 0.01;
    private static final double DEFAULT_PERCENTAGE = 100;
    private static double convergenceThreshold;
    private static double convergencePercentage;

    public static void run(FlameContext context, String[] args) {
        String coordinator;
        try {
            coordinator = context.getKVS().getCoordinator();
            context.getKVS().delete("pt-pageranks");
            context.getKVS().persist("pt-pageranks");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        convergenceThreshold = args.length >= 1 ? Double.parseDouble(args[0]) : DEFAULT_THRESHOLD;
        convergencePercentage = args.length >= 2 ? Double.parseDouble(args[1]) : DEFAULT_PERCENTAGE;
        convergencePercentage /= 100;

        try {
            FlameRDD rdd = context.fromTable("pt-crawl", row -> {
                String url = row.get("url");
                String page = row.get("rawHTML");
                String responseCode = row.get("responseCode");
                if (!isCrawlValid(url, page, responseCode)) {
                    return null;
                }
                return url + "\t" + page;
            });

            FlamePairRDD stateTable = rdd.mapToPair(str -> {
                String[] parts = str.split("\t", 2);
                String url = parts[0];
                String page = parts[1];
                List<String> urlList = URLExtractor.extractNormalize(page, url);
                Set<String> uniqueUrls = new HashSet<>(urlList);
                StringBuilder urlsb = new StringBuilder("1.0,1.0,");
                for (String u : uniqueUrls) {
                    urlsb.append(u).append(",");
                }
                urlsb.deleteCharAt(urlsb.length() - 1);
                return new FlamePair(url, urlsb.toString());
            });

            while (true) {

                FlamePairRDD transferTable = getTransferTable(stateTable);

                FlamePairRDD aggregatedTransferTable = aggregate(transferTable);

                FlamePairRDD updatedStateTable = updateStateTable(stateTable, aggregatedTransferTable);

                FlameRDD flatRdd = updatedStateTable.flatMap(pair -> {
                    String[] values = pair._2().split(",");
                    double currentRank = Double.parseDouble(values[0]);
                    double previousRank = Double.parseDouble(values[1]);
                    double diff = Math.abs(previousRank - currentRank);
                    return Collections.singleton(String.valueOf(diff));
                });

                String maxRankDiff = flatRdd.fold(String.valueOf(Double.MIN_VALUE), (val1, val2) ->
                        String.valueOf(Math.max(Double.parseDouble(val1), Double.parseDouble(val2))));

                stateTable = updatedStateTable;
                if (Double.parseDouble(maxRankDiff) < convergenceThreshold) {
                    break; // updatedStateTable
                }

                double numOverThreshold = getCountOverThreshold(flatRdd);

                if (numOverThreshold / (double) flatRdd.count() > convergencePercentage) {
                    break;
                }
            }

            // normalize page rank to make the value more sparse
            stateTable = normalizePageRankScores(stateTable);

            stateTable.flatMapToPair(pair -> {
                String url = pair._1();
                String values = pair._2();
                double currentRank = Double.parseDouble(values.substring(0, values.indexOf(",")));
                KVSClient kvs = new KVSClient(coordinator);
                kvs.put("pt-pageranks", url, "pagerank", String.valueOf(currentRank));
                return new ArrayList<>();
            });

            context.output("OK");
        } catch (Exception e) {
            logger.info(e.getMessage());
        }
    }

    public static FlamePairRDD getTransferTable(FlamePairRDD stateTable) throws Exception {
        return stateTable.flatMapToPair(pair -> {
            String sourceUrl = pair._1();
            String[] parts = pair._2().split(",");
            double currentRank = Double.parseDouble(parts[0]);

            List<FlamePair> transfer = new ArrayList<>();

            double rankForLinks = currentRank * DECAY_FACTOR / (parts.length - 2);
            for (int i = 2; i < parts.length; i++) {
                transfer.add(new FlamePair(parts[i], String.valueOf(rankForLinks)));
            }
            if (!pair._2().contains(sourceUrl)) {
                transfer.add(new FlamePair(sourceUrl, String.valueOf(0.0)));
            }
            return transfer;
        });
    }

    public static FlamePairRDD aggregate(FlamePairRDD transferTable) throws Exception {
        return transferTable.foldByKey("0", (sum, value) ->
                String.valueOf(Double.parseDouble(sum) + Double.parseDouble(value)));
    }

    public static FlamePairRDD updateStateTable(FlamePairRDD stateTable, FlamePairRDD aggregatedTransferTable) throws Exception {
        return stateTable.join(aggregatedTransferTable).flatMapToPair(pair -> {
            String sourceUrl = pair._1();
            String values = pair._2();

            int firstInx = values.indexOf(',');
            double currentRank = Double.parseDouble(values.substring(0, firstInx));
            int lastInx = values.lastIndexOf(',');
            double newRank = Double.parseDouble(values.substring(lastInx + 1));

            int secondInx = values.indexOf(',', lastInx + 1);
            double updatedRank = newRank + (1 - DECAY_FACTOR);
            if (secondInx >= lastInx) {
                return Collections.singleton(new FlamePair(sourceUrl,
                        updatedRank + "," + currentRank));
            }
            String outLinks = values.substring(secondInx + 1, lastInx);
            return Collections.singleton(new FlamePair(sourceUrl,
                    updatedRank + "," + currentRank + "," + outLinks));
        });
    }

    private static FlamePairRDD normalizePageRankScores(FlamePairRDD stateTable) throws Exception {
        // First extract scores and calculate min/max
        FlamePairRDD scoreStats = stateTable.flatMapToPair(pair -> {
            try {
                String values = pair._2();
                // Split by comma and get current rank (first element)
                String[] parts = values.split(",");
                if (parts.length < 2) {
                    logger.error("Invalid state format: " + values);
                    return Collections.emptyList();
                }
                double currentRank = Double.parseDouble(parts[0]);

                List<FlamePair> stats = new ArrayList<>();
                stats.add(new FlamePair("max", String.valueOf(currentRank)));
                stats.add(new FlamePair("min", String.valueOf(currentRank)));
                return stats;
            } catch (Exception e) {
                logger.error("Error processing pair: " + pair._1() + ", " + pair._2(), e);
                return Collections.emptyList();
            }
        });

        // Get min and max using foldByKey
        FlamePairRDD reducedStats = scoreStats.foldByKey("0.0", (acc, value) -> {
            if (acc.equals("0.0")) return value;
            double existing = Double.parseDouble(acc);
            double current = Double.parseDouble(value);
            return String.valueOf(acc.startsWith("max") ?
                    Math.max(existing, current) : Math.min(existing, current));
        });

        // Extract final min/max values
        List<FlamePair> statsList = reducedStats.collect();
        double maxScore = Double.MIN_VALUE;
        double minScore = Double.MAX_VALUE;

        for (FlamePair stat : statsList) {
            try {
                double value = Double.parseDouble(stat._2());
                if (stat._1().equals("max")) {
                    maxScore = Math.max(maxScore, value);
                } else if (stat._1().equals("min")) {
                    minScore = Math.min(minScore, value);
                }
            } catch (Exception e) {
                logger.error("Error parsing stat: " + stat._1() + ", " + stat._2(), e);
            }
        }

        // Validate min/max values
        if (maxScore == Double.MIN_VALUE || minScore == Double.MAX_VALUE) {
            logger.error("Failed to calculate valid min/max scores");
            return stateTable;
        }

        // If scores are too close together, distribute them evenly
        if (Math.abs(maxScore - minScore) < 1e-10) {
            return distributeScoresEvenly(stateTable);
        }

        // Normalize to [0.1, 1.0] range
        final double range = maxScore - minScore;
        final double finalMinScore = minScore;
        return stateTable.flatMapToPair(pair -> {
            try {
                String[] parts = pair._2().split(",");
                if (parts.length < 2) return Collections.emptyList();

                double currentRank = Double.parseDouble(parts[0]);
                double normalizedRank = 0.1 + 0.9 * (currentRank - finalMinScore) / range;

                // Preserve the rest of the state information
                StringBuilder newValue = new StringBuilder();
                newValue.append(String.format("%.6f", normalizedRank));
                for (int i = 1; i < parts.length; i++) {
                    newValue.append(",").append(parts[i]);
                }

                return Collections.singleton(new FlamePair(pair._1(), newValue.toString()));
            } catch (Exception e) {
                logger.error("Error normalizing pair: " + pair._1() + ", " + pair._2(), e);
                return Collections.emptyList();
            }
        });
    }

    private static FlamePairRDD distributeScoresEvenly(FlamePairRDD stateTable) throws Exception {
        List<FlamePair> pairs = stateTable.collect();
        pairs.sort((a, b) -> a._1().compareTo(b._1()));

        int totalUrls = pairs.size();
        if (totalUrls <= 1) return stateTable;

        double step = 0.9 / (totalUrls - 1);
        Map<String, Double> distributedScores = new HashMap<>();

        for (int i = 0; i < totalUrls; i++) {
            distributedScores.put(pairs.get(i)._1(), 0.1 + (i * step));
        }

        return stateTable.flatMapToPair(pair -> {
            try {
                String[] parts = pair._2().split(",");
                if (parts.length < 2) return Collections.emptyList();

                Double score = distributedScores.get(pair._1());
                if (score == null) return Collections.emptyList();

                // Preserve the rest of the state information
                StringBuilder newValue = new StringBuilder();
                newValue.append(String.format("%.6f", score));
                for (int i = 1; i < parts.length; i++) {
                    newValue.append(",").append(parts[i]);
                }

                return Collections.singleton(new FlamePair(pair._1(), newValue.toString()));
            } catch (Exception e) {
                logger.error("Error distributing pair: " + pair._1() + ", " + pair._2(), e);
                return Collections.emptyList();
            }
        });
    }

    public static boolean isCrawlValid(String url, String page, String responseCode) {
        return url == null ||
               url.isEmpty() ||
               page == null ||
               page.isEmpty() ||
               url.contains("..") ||
               url.contains("...") ||
               url.contains(" ") ||
               url.contains(",") ||
               responseCode == null ||
               responseCode.isEmpty() ||
               ("200").equals(responseCode);
    }

    public static double getCountOverThreshold(FlameRDD rdd) throws Exception {
        FlamePairRDD rankOverThreshold = rdd.mapToPair(str ->
                new FlamePair("rankDifference", Double.parseDouble(str) < convergenceThreshold ? "1" : "0"));

        FlamePairRDD numOverThresholdRDD = rankOverThreshold.foldByKey("0", (sum, value) ->
                String.valueOf(Integer.parseInt(sum) + Integer.parseInt(value)));

        List<FlamePair> overThreshold = numOverThresholdRDD.collect();
        double numOverThreshold = 0;
        for (FlamePair pair : overThreshold) {
            numOverThreshold += Double.parseDouble(pair._2());
        }
        return numOverThreshold;
    }

    public static class URLExtractor {
        private static final Pattern LINK_PATTERN = Pattern.compile(
                "<a\\s+[^>]*href=['\"]?([^'\">\\s]+)['\"]?", Pattern.CASE_INSENSITIVE);

        public static List<String> extractNormalize(String page, String baseUrl) {
            if (page == null || baseUrl == null) {
                return new ArrayList<>();
            }

            List<String> extractedUrls = extractURLs(page);
            List<String> normalizedUrls = new ArrayList<>();
            for (String url : extractedUrls) {
                String normalizedUrl = normalizeURL(url, baseUrl);
                if (normalizedUrl != null) {
                    normalizedUrls.add(normalizedUrl);
                }
            }
            return normalizedUrls;
        }

        public static List<String> extractURLs(String html) {
            List<String> urls = new ArrayList<>();
            if (html == null || html.trim().isEmpty()) {
                return urls;
            }

            try {
                Matcher matcher = LINK_PATTERN.matcher(html);
                while (matcher.find()) {
                    String url = matcher.group(1);
                    if (url != null && !url.trim().isEmpty()) {
                        urls.add(url.trim());
                    }
                }
            } catch (Exception e) {
                logger.error("Error extracting URLs: " + e.getMessage(), e);
            }
            return urls;
        }

        public static String normalizeURL(String url, String baseUrl) {
            try {
                int fragmentIndex = url.indexOf('#');
                if (fragmentIndex != -1) {
                    url = url.substring(0, fragmentIndex);
                }

                if (url.isEmpty()) {
                    return null;
                }

                String[] urlParts = URLParser.parseURL(url);
                String[] baseParts = baseUrl != null ? URLParser.parseURL(baseUrl) : null;

                String protocol = urlParts[0];
                if (protocol == null) {
                    protocol = baseParts[0] == null ? "http" : baseParts[0];
                }

                String host = urlParts[1];
                if (host == null) {
                    host = baseParts[1];
                }

                String port = urlParts[2];
                if (port == null) {
                    if (baseParts[2] != null) {
                        port = baseParts[2];
                    } else {
                        port = protocol.equalsIgnoreCase("http") ? "80" : "443";
                    }
                }

                String path = urlParts[3];
                if (path == null || path.isEmpty()) {
                    path = "/";
                }

                if (!path.startsWith("/")) {
                    String basePath = baseParts != null && baseParts[3] != null ? baseParts[3] : "/";
                    int lastSlash = basePath.lastIndexOf('/');
                    if (lastSlash != -1) {
                        basePath = basePath.substring(0, lastSlash + 1);
                    }
                    path = basePath + path;
                }

                if (path.contains("//")) {
                    path = path.replaceAll("//", "/");
                }

                Deque<String> pathParts = new ArrayDeque<>();
                String[] parts = path.split("/");
                for (String part : parts) {
                    if (part.isEmpty() || ".".equals(part)) {
                        continue;
                    }
                    if ("..".equals(part)) {
                        if (!pathParts.isEmpty()) {
                            pathParts.removeLast();
                        }
                    } else {
                        pathParts.addLast(part);
                    }
                }

                StringBuilder sb = new StringBuilder();
                for (String part : pathParts) {
                    sb.append("/").append(part);
                }

                path = sb.isEmpty() ? "/" : sb.toString();

                String normalizedUrl = protocol + "://" + host + ":" + port + path;

                if (shouldFilterUrl(normalizedUrl)) {
                    return null;
                }

                return normalizedUrl;
            } catch (Exception e) {
                logger.info("Error normalizing URL: " + e.getMessage());
                return null;
            }
        }

        private static boolean shouldFilterUrl(String url) {
            return !(url.startsWith("http://") || url.startsWith("https://"))
                    || url.endsWith(".jpg") || url.endsWith(".jpeg") || url.endsWith(".gif")
                    || url.endsWith(".png") || url.endsWith(".txt");
        }

    }

}
