package cis5550.jobs;

import cis5550.external.PorterStemmer;
import cis5550.flame.*;
import cis5550.tools.Hasher;
import cis5550.tools.Logger;
import cis5550.kvs.*;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.CharArraySet;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.StringReader;
import java.net.URLDecoder;
import java.util.*;
import java.util.regex.Pattern;
import java.util.regex.Matcher;
import java.util.stream.Collectors;

public class Indexer {
    private static final Logger logger = Logger.getLogger(Indexer.class);

    private static final String BODY_OPEN_TAG = "<body";
    private static final String BODY_CLOSE_TAG = "</body>";
    private static final String TITLE_OPEN_TAG = "<title";
    private static final String TITLE_CLOSE_TAG = "</title>";

    private static final Pattern PATTERN = Pattern.compile(
            "<meta\\s+name\\s*=\\s*\"description\"[^>]*content\\s*=\\s*\"([^\"]+)\"",
            Pattern.DOTALL | Pattern.CASE_INSENSITIVE
    );

    private static final CharArraySet STOP_WORDS =
            new CharArraySet(Arrays.asList("a", "about", "above", "across",
                    "after", "afterwards", "again", "against", "all", "almost", "alone",
                    "along", "already", "also", "although", "always", "am", "among",
                    "amongst", "amoungst", "amount", "an", "and", "another", "any",
                    "anyhow", "anyone", "anything", "anyway", "anywhere", "are", "around",
                    "as", "at", "back", "be", "became", "because", "become", "becomes",
                    "becoming", "been", "before", "beforehand", "behind", "being", "below",
                    "beside", "besides", "between", "beyond", "bill", "both", "bottom",
                    "but", "by", "call", "can", "cannot", "cant", "co", "computer", "con",
                    "could", "couldnt", "cry", "de", "describe", "detail", "do", "done",
                    "down", "due", "during", "each", "eg", "eight", "either", "eleven",
                    "else", "elsewhere", "empty", "enough", "etc", "even", "ever", "every",
                    "everyone", "everything", "everywhere", "except", "few", "fifteen",
                    "fify", "fill", "find", "fire", "first", "five", "for", "former",
                    "formerly", "forty", "found", "four", "from", "front", "full",
                    "further", "get", "give", "go", "had", "has", "hasnt", "have", "he",
                    "hence", "her", "here", "hereafter", "hereby", "herein", "hereupon",
                    "hers", "herse", "him", "himse", "his", "how", "however", "hundred",
                    "i", "ie", "if", "in", "inc", "indeed", "interest", "into", "is", "it",
                    "its", "itse", "keep", "last", "latter", "latterly", "least", "less",
                    "ltd", "made", "many", "may", "me", "meanwhile", "might", "mill",
                    "mine", "more", "moreover", "most", "mostly", "move", "much", "must",
                    "my", "myse", "name", "namely", "neither", "never", "nevertheless",
                    "next", "nine", "no", "nobody", "none", "noone", "nor", "not",
                    "nothing", "now", "nowhere", "of", "off", "often", "on", "once", "one",
                    "only", "onto", "or", "other", "others", "otherwise", "our", "ours",
                    "ourselves", "out", "over", "own", "part", "per", "perhaps", "please",
                    "put", "rather", "re", "same", "see", "seem", "seemed", "seeming",
                    "seems", "serious", "several", "she", "should", "show", "side", "since",
                    "sincere", "six", "sixty", "so", "some", "somehow", "someone",
                    "something", "sometime", "sometimes", "somewhere", "still", "such",
                    "system", "take", "ten", "than", "that", "the", "their", "them",
                    "themselves", "then", "thence", "there", "thereafter", "thereby",
                    "therefore", "therein", "thereupon", "these", "they", "thick", "thin",
                    "third", "this", "those", "though", "three", "through", "throughout",
                    "thru", "thus", "to", "together", "too", "top", "toward", "towards",
                    "twelve", "twenty", "two", "un", "under", "until", "up", "upon", "us",
                    "very", "via", "was", "we", "well", "were", "what", "whatever", "when",
                    "whence", "whenever", "where", "whereafter", "whereas", "whereby",
                    "wherein", "whereupon", "wherever", "whether", "which", "while",
                    "whither", "who", "whoever", "whole", "whom", "whose", "why", "will",
                    "with", "within", "without", "would", "yet", "you", "your", "yours",
                    "yourself", "yourselves"
            ), true);

    public static void run(FlameContext context, String[] args) {
//        KVSClient kvs = null;
        try {
//            kvs = context.getKVS();
            String coordinator = context.getKVS().getCoordinator();

            FlameRDD crawledRdd = context.fromTable("pt-crawl", row -> {
                String url = row.get("url");
                String page = row.get("page");
                String responseCode = row.get("responseCode");
                if (!isCrawlValid(url, page, responseCode)) {
                    return null;
                }
                return url + "\t" + page;
            });

            context.getKVS().persist("pt-index");
            context.getKVS().persist("pt-pages");

            FlamePairRDD urlPagePairs = crawledRdd.mapToPair(data -> {
                String[] parts = data.split("\t", 2);
                return new FlamePair(parts[0], parts[1]);
            });

            urlPagePairs.flatMapToPair(pair -> {
                String url = pair._1();
                String page = pair._2();
                String urlHash = Hasher.hash(url).substring(0, 12);

                KVSClient kvs = new KVSClient(coordinator);

                // Extract features
                String body = extractHTMLBody(page);
                String title = extractHTMLTitle(page, url);
                String meta = extractHTMLMeta(page);

                // Write to KVS
                if (body != null && !body.isEmpty()) {
                    kvs.put("pt-pages", urlHash, "url", url);
                    kvs.put("pt-pages", urlHash, "body-word-count",
                            String.valueOf(body.split(" ").length));
                    kvs.put("pt-pages", urlHash, "title", title);
                    kvs.put("pt-pages", urlHash, "meta", meta);
                }

                Map<String, List<String>> wordPositions = processWithLucene(page);

                Map<String, String> updates = new HashMap<>();
                for (Map.Entry<String, List<String>> entry : wordPositions.entrySet()) {
                    String word = entry.getKey();
                    List<String> positions = entry.getValue();
                    String urlPosition = urlHash + ":" + String.join("|", positions);
                    updates.put(word, urlPosition);
                }

                // Then do batch update
                synchronized (kvs) {
                    for (Map.Entry<String, String> update : updates.entrySet()) {
                        String word = update.getKey();
                        String newPosition = update.getValue();
                        if (kvs.existsRow("pt-index", word)) {
                            Row wordRow = kvs.getRow("pt-index", word);
                            String existingPositions = wordRow.get("url-position");
                            newPosition = newPosition + "," + existingPositions;
                        }
                        kvs.put("pt-index", word, "url-position", newPosition);
                    }
                }
                return new ArrayList<>();
            });
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
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

    public static String extractHTMLBody(String page) {
        String body;
        int bodyStartIndex = page.indexOf(BODY_OPEN_TAG);
        int bodyEndIndex = page.contains(BODY_OPEN_TAG) && page.contains(BODY_CLOSE_TAG) ?
                page.indexOf(BODY_CLOSE_TAG) : Math.min(page.length() - bodyStartIndex, page.length());

        if (bodyStartIndex == -1) {
            return page;
        }
        bodyStartIndex = page.indexOf(">", bodyStartIndex) + 1;

        // If </body> is missing, use the rest of the content
        if (bodyEndIndex == -1) {
            bodyEndIndex = page.length();
        }

        // Extract and return the content between <body> and </body>
        if (bodyStartIndex >= bodyEndIndex) {
            return "";
        }
        return page.substring(bodyStartIndex, bodyEndIndex).trim();
    }

    public static String extractHTMLTitle(String page, String url) {
        int titleStartIndex = page.indexOf(TITLE_OPEN_TAG);
        int titleEndIndex = page.contains(TITLE_CLOSE_TAG) ? page.indexOf(TITLE_CLOSE_TAG) : -1;

        // Default title to URL if not found
        if (titleStartIndex == -1) {
            return url;
        }
        // Extract the content between <title> and </title>
        String title = "";
        if (titleEndIndex == -1 || titleEndIndex < titleStartIndex) {
            String intro = page.substring(0, titleStartIndex);
            int lastClosingAngleBracket = intro.lastIndexOf(">");
            if (lastClosingAngleBracket != -1) {
                title = page.substring(lastClosingAngleBracket + 1).trim();
            } else {
                title = url; // Fallback to URL if content is ambiguous
            }
        } else {
            // Extract the content and skip over attributes
            String titleContent = page.substring(titleStartIndex + TITLE_OPEN_TAG.length(), Math.min(titleEndIndex, page.length()));
            int closeBracketIndex = titleContent.indexOf(">");
            if (closeBracketIndex != -1) {
                titleContent = titleContent.substring(closeBracketIndex + 1);
            }
            title = titleContent.trim();
        }

        return title.isEmpty() ? url : title;
    }

    public static String extractHTMLMeta(String page) {
        if (page == null || page.isEmpty()) {
            return ""; // Return empty string if page content is null or empty
        }
        Matcher matcher = PATTERN.matcher(page);
        // Find the first match and extract the content value
        if (matcher.find()) {
            return Optional.ofNullable(matcher.group(1)).orElse("").trim();
        }
        return "";
    }

    // Process text with Lucene to extract words and positions
    public static Map<String, List<String>> processWithLucene(String pageContent) {
        Map<String, List<String>> wordPositions = new HashMap<>();
        try (Analyzer analyzer = new StandardAnalyzer(STOP_WORDS)) {
            try (var tokenStream = analyzer.tokenStream("field", new StringReader(pageContent))) {
                tokenStream.reset();

                CharTermAttribute charTermAttr = tokenStream.addAttribute(CharTermAttribute.class);
                int position = 0;

                while (tokenStream.incrementToken()) {
                    String word = charTermAttr.toString();
                    String stemmedWord = stem(word);
                    if (!stemmedWord.equals(word)) {
                        wordPositions.computeIfAbsent(stemmedWord, k -> new ArrayList<>()).add(String.valueOf(position));
                    }
                    wordPositions.computeIfAbsent(word, k -> new ArrayList<>()).add(String.valueOf(position));
                    position++;
                }
                tokenStream.end();
            }
        } catch (Exception e) {
            logger.error("Error processing text with Lucene: " + e.getMessage(), e);
        }
        return wordPositions;
    }

    public static String stem(String word) {
        PorterStemmer stemmer = new PorterStemmer();
        for (char c : word.toCharArray()) {
            stemmer.add(c);
        }
        stemmer.stem();
        return stemmer.toString();
    }
}