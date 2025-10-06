package cis5550.jobs;

import cis5550.flame.FlameContext;
import cis5550.flame.FlameContextImpl;
import cis5550.flame.FlameRDD;
import cis5550.flame.FlameRDDImpl;
import cis5550.kvs.KVSClient;
import cis5550.kvs.Row;
import cis5550.tools.Hasher;
import cis5550.tools.Logger;
import cis5550.tools.URLParser;

import java.io.*;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;


public class Crawler {

    private static final Logger logger = Logger.getLogger(Crawler.class);
    private static final int MAX_RETRIES = 3;
    private static final long RETRY_DELAY = 1000; // 1秒
    private static final long DEFAULT_DELAY = 1000;
    private static final AtomicLong crawledCount = new AtomicLong(0);
    private static final AtomicLong errorCount = new AtomicLong(0);
    private static final AtomicLong lastSuccessfulCrawl = new AtomicLong(System.currentTimeMillis());

    private static final Map<String, Object> hostLocks = new ConcurrentHashMap<>();
    private static Object getHostLock(String host) {
        return hostLocks.computeIfAbsent(host, k -> new Object());
    }


    private static final Set<Integer> REDIRECTS_CODE = new HashSet<>();
    static {
        REDIRECTS_CODE.add(301);
        REDIRECTS_CODE.add(302);
        REDIRECTS_CODE.add(303);
        REDIRECTS_CODE.add(307);
        REDIRECTS_CODE.add(308);
    }

    public static void run(FlameContext context, String[] args) throws Exception{
        if (args == null || args.length < 1) {
            context.output("Error: no seed URL input");
            return;
        }

        // clear out the old content seen table prevent data contamination from hash collision
        KVSClient kvs = context.getKVS();
        String coordinator = kvs.getCoordinator();

        String seedURL = normalizeURL(args[0], args[0]);
        if (seedURL == null) {
            context.output("Error: invalid seed URL");
        }

        kvs.persist("pt-crawl");

        FlameRDD urlQueue = context.parallelize(Collections.singletonList(seedURL));

        int iterationCount = 0;
        while (urlQueue.count() > 0) {
            iterationCount++;
            if (iterationCount % 100 == 0) { // 每100次迭代打印一次统计信息
                logger.info(String.format(
                        "Crawling statistics - Iteration: %d, Crawled: %d, Errors: %d, Success rate: %.2f%%",
                        iterationCount,
                        crawledCount.get(),
                        errorCount.get(),
                        (float)crawledCount.get() * 100 / (crawledCount.get() + errorCount.get())
                ));
            }
            if (System.currentTimeMillis() - lastSuccessfulCrawl.get() > 60000) { // 1分钟没有成功的抓取
                logger.error("爬虫可能已卡住 - 最后成功抓取时间: " +
                        new Date(lastSuccessfulCrawl.get()));
            }
            lastSuccessfulCrawl.set(System.currentTimeMillis());
            FlameRDD nextQueue = urlQueue.flatMap(new FlameRDD.StringToIterable() {
                @Override
                public Iterable<String> op(String url) throws Exception {
                    List<String> results = new ArrayList<>();
                    try {
//                        logger.info("---Start crawling " + url);
                        KVSClient kvs = new KVSClient(coordinator);

                        if (null == url) {
                            return results;
                        }

                        // early check for url validation
                        if (!url.startsWith("http://") && !url.startsWith("https://")) {
                            logger.info("Skipping invalid URL: " + url);
                            return new ArrayList<>();
                        }

                        URI uri = new URI(url);
                        URL urlObj = uri.toURL();

                        String urlHash = Hasher.hash(url);
                        if (kvs.existsRow("pt-crawl", urlHash)) {
                            logger.info("Skipping already crawled URL: " + url);
                            return new ArrayList<>();
                        }

                        // host table in KVS
                        String host = urlObj.getHost();
                        Row hostRow = kvs.getRow("hosts", host);
                        if (hostRow == null) {
                            hostRow = new Row(host);
                            hostRow.put("lastAccessTime", getTimeStamp());
                        }

                        // Get request for robots.txt and parse robots.txt file
                        RobotsRule rule = handleRobots(kvs, urlObj, hostRow);

                        // check if url is allowed
                        if (!isAllowed(rule, urlObj.getPath())) {
                            return new ArrayList<>();
                        }

                        float delay = rule.crawlDelay != null ? rule.crawlDelay * 1000 : 0;

//                        logger.info("---Getting LastAccessTime---");
                        // last access time for current host
                        String lastAccessTime = hostRow.get("lastAccessTime");
                        if (lastAccessTime == null || !isValidTimeStamp(lastAccessTime)) {
                            hostRow.put("lastAccessTime", getTimeStamp());
                        }
                        if (lastAccessTime != null) {
                            if (System.currentTimeMillis() - Long.parseLong(lastAccessTime) < delay) {
                                results.add(url);
                                return results;
                            }
                        }

//
//                        if (lastAccessTime != null && rule.crawlDelay != null && isValidTimeStamp(lastAccessTime)) {
//                            long currentTime = System.currentTimeMillis();
//                            long lastAccess = Long.parseLong(lastAccessTime);
//                            long waitTime = (long) Math.max(0, delay - (currentTime - lastAccess));
//                            if (waitTime > 0) {
//                                logger.info("等待 " + waitTime + "ms 以遵守抓取延迟");
//                                Thread.sleep(waitTime);
//                            }
//                        }
//                        logger.info("---lastAccessTime: " + lastAccessTime);

                        Row row = new Row(urlHash);
                        row.put("url", url);

                        // send HEAD request
                        HttpURLConnection headConn = sendRequest("HEAD", urlObj);

                        int responseCode = headConn.getResponseCode();

                        String contentType = headConn.getHeaderField("Content-Type").split(";")[0];
                        if (contentType != null) {
                            row.put("contentType", contentType);
                        }
                        String contentLength = headConn.getHeaderField("Content-Length");
                        if (contentLength != null && contentLength.matches("\\d+")) {
                            row.put("length", contentLength);
                        }
                        else {
                            row.put("length", "0"); // Optional: Default to 0 if content length is unavailable or invalid
                        }

                        if (responseCode != 200) {
                            row.put("responseCode", String.valueOf(responseCode));
//                            logger.info("HEAD response code: " + responseCode);
                            kvs.putRow("pt-crawl", row);
                            hostRow.put("lastAccessTime", String.valueOf(System.currentTimeMillis()));
                            kvs.putRow("hosts", hostRow);
                            if (REDIRECTS_CODE.contains(responseCode)) {
                                String redirectUrl = headConn.getHeaderField("Location");
                                if (redirectUrl != null) {
                                    redirectUrl = normalizeURL(redirectUrl, url);
                                    if (redirectUrl != null && !redirectUrl.equals(url)) {
                                        results.add(redirectUrl);
//                                        logger.info("Following redirect to: " + redirectUrl);
                                    }
                                }
                            }
                            return results;
                        }
                        headConn.disconnect();

                        // send GET request
                        HttpURLConnection getConn = sendRequest("GET", urlObj);

                        responseCode = getConn.getResponseCode();
                        row.put("responseCode", String.valueOf(responseCode));
//                        logger.info("GET response code: " + responseCode);
//                        logger.info("contentTypecontentTypecontentType\n");
//                        logger.info(contentType);
                        if (responseCode == 200 && ("text/html".equals(contentType))) {

                            String page = readPageContent(getConn);
//                            logger.info("pagepagepagepagepagepage\n");
//                            logger.info(page);

                            // start

                            row.put("page", filterPage(page));

                            row.put("url", url);
                            row.put("rawHTML", page);

//                            logger.info("---Start extracting URLS---");

                            List<String> extractedUrls = URLExtractor.extractURLs(page);


//                            logger.info("Found " + extractedUrls.size() + " URLs on page " + url);

                            // add extracted URL to the result without duplicate
                            for (String extractedUrl : extractedUrls) {
                                String normalizedUrl = normalizeURL(extractedUrl, url);
                                if (normalizedUrl != null && !normalizedUrl.equals(url)
                                        && !results.contains(normalizedUrl) && shouldAddToQueue(normalizedUrl)) {
                                    results.add(normalizedUrl);
//                                    logger.info("Added new URL to queue: " + normalizedUrl);
                                }
                            }
                        }
                        kvs.putRow("pt-crawl", row);

                        hostRow.put("lastAccessTime", String.valueOf(System.currentTimeMillis()));

                        kvs.putRow("hosts", hostRow);

                        getConn.disconnect();
                        return results;
                    } catch (Exception e) {
                        logger.info("Error processing URL " + url + ": " + e.getMessage());
                        return new ArrayList<>();
                    }
                }

            });
            // Collect all URLs into a list
            List<String> allUrls = nextQueue.collect();

            // Manually repartition and create a new RDD
            List<List<String>> partitions = partitionList(allUrls, context.getWorkerCount());
            List<String> combinedUrls = new ArrayList<>();
            for (List<String> partition : partitions) {
                combinedUrls.addAll(partition);
            }
            urlQueue = context.parallelize(combinedUrls);
        }
        context.output("OK");
    }

    public static <T> List<List<T>> partitionList(List<T> list, int numPartitions) {
        List<List<T>> partitions = new ArrayList<>();
        int partitionSize = (int) Math.ceil((double) list.size() / numPartitions);
        for (int i = 0; i < list.size(); i += partitionSize) {
            partitions.add(list.subList(i, Math.min(i + partitionSize, list.size())));
        }
        return partitions;
    }

    public static HttpURLConnection sendRequest(String method, URL url) throws IOException {
        HttpURLConnection conn = null;
        for (int i = 0; i < MAX_RETRIES; i++) {
            try {
//                logger.info("发送请求: " + method + " " + url + " (尝试 " + (i+1) + "/" + MAX_RETRIES + ")");
                conn = (HttpURLConnection) url.openConnection();
                conn.setRequestMethod(method);
                conn.setRequestProperty("User-Agent", "cis5550-crawler");
                conn.setInstanceFollowRedirects(false);
                conn.setConnectTimeout(5000);
                conn.setReadTimeout(5000);
                return conn;
            } catch (IOException e) {
                logger.error("请求失败: " + e.getMessage());
                if (conn != null) {
                    conn.disconnect();
                }
                if (i == MAX_RETRIES - 1) throw e;
                logger.info("等待 " + RETRY_DELAY + "ms 后重试");
                try {
                    Thread.sleep(RETRY_DELAY);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new IOException("重试过程中被中断", ie);
                }
            }
        }
        throw new IOException("在 " + MAX_RETRIES + " 次尝试后失败");
    }

    public static RobotsRule handleRobots(KVSClient kvs, URL url, Row hostRow) throws Exception {
        String host = url.getHost();
        Object hostLock = getHostLock(host);

        // 设置获取锁的超时时间
        long lockTimeout = 5000; // 5秒超时
        long startTime = System.currentTimeMillis();

        synchronized (hostLock) {
            try {
                // 添加超时检查
                if (System.currentTimeMillis() - startTime > lockTimeout) {
                    logger.warn("获取主机锁超时: " + host);
                    return new RobotsRule();
                }

                String robotsContent = hostRow.get("robotsContent");
                String encodedRobotsContent = "";
                if (robotsContent == null) {
                    URI robotsUri = new URI(url.getProtocol(), host, "/robots.txt", null);
                    URL robotsUrl = robotsUri.toURL();

                    HttpURLConnection robotsConn = null;
                    try {
                        robotsConn = sendRequest("GET", robotsUrl);
                        int robotsResponseCode = robotsConn.getResponseCode();

                        if (robotsResponseCode == 200) {
                            robotsContent = readRobots(robotsConn);
                        } else {
                            robotsContent = "";
                            logger.info("Robots.txt 不可访问，使用空规则");
                        }
                    } catch (Exception e) {
                        logger.error("获取 robots.txt 失败: " + e.getMessage());
                        return new RobotsRule();
                    } finally {
                        if (robotsConn != null) {
                            robotsConn.disconnect();
                        }
                    }

                    try {
                        Row newHostRow = new Row(host);
                        for (String col : hostRow.columns()) {
                            newHostRow.put(col, hostRow.get(col));
                        }
                        encodedRobotsContent = Base64.getEncoder().encodeToString(robotsContent.getBytes(StandardCharsets.UTF_8));
                        newHostRow.put("robotsContent", encodedRobotsContent);
                        newHostRow.put("lastAccessTime", String.valueOf(System.currentTimeMillis()));
                        kvs.putRow("hosts", newHostRow);
                    } catch (Exception e) {
                        logger.error("更新主机信息失败: " + e.getMessage());
                    }
                } else {
                    try {
                        robotsContent = new String(Base64.getDecoder().decode(robotsContent), StandardCharsets.UTF_8);
                    } catch (IllegalArgumentException e) {
                        logger.info("RobotsContent is not Base64 encoded, using raw content.");
                    }
                }
                return parseRobots(robotsContent, "cis5550-crawler");
            } catch (Exception e) {
                errorCount.incrementAndGet();
                logger.error("处理 robots.txt 时发生错误: " + url, e);
                return new RobotsRule();
            }
        }
    }

    public static String readPageContent(HttpURLConnection conn) throws IOException {
        ByteArrayOutputStream os = new ByteArrayOutputStream();
        try (InputStream is = conn.getInputStream()) {
            byte[] bufferedReader = new byte[1024];
            int bytesRead;
            while ((bytesRead = is.read(bufferedReader)) != -1) {
                os.write(bufferedReader, 0, bytesRead);
            }
            return os.toString(StandardCharsets.UTF_8);
        } catch (IOException e) {
            logger.info(" Failed reading page content");
            return "";
        }
    }


    public static String filterPage(String pageContent) throws IOException {
        // Remove non-text tags
        pageContent = pageContent.replaceAll(
                "(?i)<(svg|audio|img|video|source|track|embed|iframe|script|style|object)[^>]*>[\\s\\S]*?</\\1>",
                "");
        // Remove remaining HTML tags, leaving only text
        pageContent = pageContent.replaceAll("(?i)<[^>]+>", "");
        // Normalize white spaces
        pageContent = pageContent.trim().replaceAll("\\s{2,}", " ");
        return pageContent;
    }

    ////////////////////////////////
    // 添加一个工具方法来处理时间戳
    private static String getTimeStamp() {
        return String.valueOf(System.currentTimeMillis());
    }

    // 在相关的地方检查和验证时间戳
    private static boolean isValidTimeStamp(String timestamp) {
        try {
            Long.parseLong(timestamp);
            return true;
        } catch (NumberFormatException e) {
            return false;
        }
    }
    ////////////////////////////////////

    public static String readRobots(HttpURLConnection conn) throws IOException {
        StringBuilder content = new StringBuilder();
        try (BufferedReader reader = new BufferedReader(
                new InputStreamReader(conn.getInputStream()))) {
            String line;
            while ((line = reader.readLine()) != null) {
                content.append(line).append("\n");
            }
        } catch (IOException e) {
            logger.info(" Failed reading robots");
            return "";
        }
        return content.toString();
    }

    public static class URLExtractor {
        private static final Pattern LINK_PATTERN = Pattern.compile(
                "<a\\s+[^>]*href=['\"]?([^'\">\\s]+)['\"]?", Pattern.CASE_INSENSITIVE);

        public static List<String> extractURLs(String html) {
            List<String> urls = new ArrayList<>();
            Matcher matcher = LINK_PATTERN.matcher(html);

            while (matcher.find()) {
                try {
                    String url = matcher.group(1);
                    if (url != null && !url.trim().isEmpty()) {
                        urls.add(url.trim());
                    }
                } catch (Exception e) {
                    continue;
                }
            }
            return urls;
        }
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


    public static boolean shouldFilterUrl(String url) {
        return !(url.startsWith("http://") || url.startsWith("https://"))
                || url.endsWith(".jpg") || url.endsWith(".jpeg") || url.endsWith(".gif")
                || url.endsWith(".png") || url.endsWith(".txt");
    }

    public static class RobotsRule {
        List<String> allows = new ArrayList<>();
        List<String> disallows = new ArrayList<>();
        Float crawlDelay = null;
    }

    public static RobotsRule parseRobots(String content, String userAgent) {
        RobotsRule rule = new RobotsRule();
        if (content == null || content.trim().isEmpty()) {
            return rule;
        }

        String[] lines = content.split("\n");
        boolean inRelevantSection = false;

        for (String line : lines) {
            line = line.trim();
            if (line.isEmpty() || line.startsWith("#")) {
                continue;
            }

            String[] parts = line.split(":", 2);
            if (parts.length != 2) {
                continue;
            }

            String key = parts[0].trim().toLowerCase();
            String value = parts[1].trim();

            switch (key) {
                case "user-agent":
                    inRelevantSection = userAgent.equals(value) || "*".equals(value);
                    break;
                case "allow":
                    if (inRelevantSection && !value.isEmpty()) {
                        if (!value.startsWith("/")) value = "/" + value;
                        rule.allows.add(value);
                    }
                    break;
                case "disallow":
                    if (inRelevantSection && !value.isEmpty()) {
                        if (!value.startsWith("/")) value = "/" + value;
                        rule.disallows.add(value);
                    }
                    break;
                case "crawl-delay":
                    if (inRelevantSection) {
                        try {
                            rule.crawlDelay = Float.parseFloat(value);
                        } catch (NumberFormatException e) {
                            logger.info("Invalid crawl delay value: " + value);
                        }
                    }
                    break;
            }
        }
        return rule;
    }

    public static boolean isAllowed(RobotsRule rule, String path) {
        if (!path.startsWith("/")) {
            path = "/" + path;
        }

        // check disallow first to prioritize block
        for (String disallowPath : rule.disallows) {
            if (path.startsWith(disallowPath)) {
//                logger.info("Path " + path + " matched Disallow rule: " + disallowPath);
                return false;
            }
        }

        for (String allowPath : rule.allows) {
            if (path.startsWith(allowPath)) {
//                logger.info("Path " + path + " matched Allow rule: " + allowPath);
                return true;
            }
        }
        return true;
    }

    // helper for filter blacklist
    public static boolean isMatchBlacklist(String url, List<String> blacklist) {
        for (String pattern : blacklist) {
            String temp = pattern.replace(".", "\\.").replace("*", ".*");
            if (url.matches(temp)) {
                return true;
            }
        }
        return false;
    }

    private static boolean shouldAddToQueue(String url) {
        return !url.matches(".*\\.(jpg|jpeg|png|gif|pdf|doc|zip|mp3|mp4|avi)$");
    }

}