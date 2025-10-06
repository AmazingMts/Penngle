package cis5550.jobs;

import cis5550.kvs.*;
import cis5550.webserver.Request;
import cis5550.webserver.Response;
import cis5550.webserver.Route;
import static cis5550.webserver.Server.*;
import java.io.IOException;
import java.net.URLDecoder;
import java.util.*;
import java.util.stream.Collectors;

public class Query{
	static String example = "http://simple.crawltest.401.cis5550.net:80/:306,http://simple.crawltest.401.cis5550.net:80/ItU5tEu.html:118,http://simple.crawltest.401.cis5550.net:80/CN9CmwX.html:9 ";

    private static final HashSet<String> STOP_WORDS =
            new HashSet<>(Arrays.asList(new String[]{"a", "about", "above", "across",
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
                    "yourself", "yourselves"}));

    private static final double PR_WEIGHT = 0.3;
    private static final double TF_IDF_WEIGHT = 0.7;

    static class QueryStruct
	{
		double freqOverall;
        double ratioOverall;
        double ratioOverTitle;
		
		public QueryStruct(int freqOverall, double ratioOverall, double ratioOverTitle) {
            this.freqOverall = freqOverall;
            this.ratioOverall = ratioOverall;
            this.ratioOverTitle = ratioOverTitle;
		}
	}

    public static void main(String[] args) throws IOException {
        //argument 0: current port argument 1: kvs port
        //e.g  8100 localhost:8000
        String port = args[0];
        String coordinator = args[1];

        KVSClient kvs = new KVSClient(coordinator);
        port(Integer.parseInt(port));
        final Double alpha = 0.5;
        get("/",(req, res)->{
            return "nothing here";
        });
        kvs.persist("pt-searchhistory");
        get("/query", new Route() {
            @Override
            public Object handle(Request req, Response res) throws Exception {
                res.header("Access-Control-Allow-Origin","*");
                res.header("Access-Control-Allow-Headers", "Origin, X-Requested-With, Content-Type, Accept");
                String query = URLDecoder.decode(req.queryParams("query"), "UTF-8").toLowerCase();
                int loadingPos = Integer.valueOf(URLDecoder.decode(req.queryParams("pos"), "UTF-8"));
                if (kvs.existsRow("pt-searchhistory", query)) {
                    Row history = kvs.getRow("pt-searchhistory", query);
                    return history.get("history");
                }
                
//            String[] words = query.split(" ");
Set<String> words = new HashSet<>(Arrays.asList(query.split("\\s+|[ \t+]")));

// remove all the stop words to speed up search
words.removeAll(STOP_WORDS);

Map<String, Map<String, QueryStruct>> tfMap = new HashMap<>();
Map<String, Double> idfMap = new HashMap<>();
Map<String, Double> hostMap = new HashMap<String, Double>();
Map<String, String> link2Title = new HashMap<>();
String ans = "";
for(String word: words){
    System.out.println("input word " + word);
    if(kvs.existsRow("pt-index", word)){
        Row curRow = kvs.getRow("pt-index", word);
        
        //use example
        //normal format
        String val = curRow.get("url-position");
        //ans += val;
        
        String[] linkRecs = val.split(",");
        for(int i = 0; i < linkRecs.length; i++){
            StringBuilder linkRecBuilder = new StringBuilder(linkRecs[i]);
            // int j = i + 1;
            // while(j < linkRecs.length && !linkRecs[j].startsWith("http"))
            // {
            // 	linkRecBuilder.append(",");
            // 	linkRecBuilder.append(linkRecs[j]);
            // 	j += 1;
            // }
            // i = j - 1;
            String linkRec = linkRecBuilder.toString();
            int lastInd = linkRec.lastIndexOf(":");
            String locations = linkRec.substring(lastInd+1, linkRec.length());
            String hashLink = linkRec.substring(0, lastInd);
            String[] freqs = locations.split("\\|");
            System.out.println("location: " + locations);
            int freqOverall = freqs.length;
            ans += freqOverall + "\n";
            double textSize = Double.valueOf(kvs.getRow("pt-pages", hashLink).get("body-word-count"));
            link2Title.put(kvs.getRow("pt-pages", hashLink).get("url"), kvs.getRow("pt-pages", hashLink).get("title"));
            ans += textSize + "\n";
            double ratioOverall = (double)freqOverall / textSize;
            String link = kvs.getRow("pt-pages", hashLink).get("url");
            Row ptpages = kvs.getRow("pt-pages",hashLink);
            double ratioOverTitle = 0;
            //calculate title ratio
            if(ptpages.get("url").compareTo(ptpages.get("title")) == 0){
                ratioOverTitle = 0;
            }else{
                int countTitle = 0;
                for(int j = 0; j < freqs.length; j++){
                    if(Integer.valueOf(freqs[j]) < Integer.valueOf(ptpages.get("title-word-count"))){
                        countTitle++;
                    }
                }
                ratioOverall = countTitle / Double.valueOf(ptpages.get("title-word-count"));
            }
            if(!tfMap.containsKey(link)){
                tfMap.put(link, new HashMap<>());
                tfMap.get(link).put("--maxFreq--", new QueryStruct(freqOverall, ratioOverall, ratioOverTitle));
            }
            tfMap.get(link).put(word, new QueryStruct(freqOverall, ratioOverall, ratioOverTitle));
            if(ratioOverall > tfMap.get(link).get("--maxFreq--").ratioOverall){
                tfMap.get(link).get("--maxFreq--").ratioOverall = ratioOverall;
            }
            // ans += "we got this word: " + locations;
        }
    }
}

//calculate tf
for (Map.Entry<String, Map<String, QueryStruct>> entry : tfMap.entrySet()) {
    String link = entry.getKey();
    Map<String, QueryStruct> wordsInDoc = entry.getValue();
    double maxFreq = wordsInDoc.get("--maxFreq--").ratioOverall;
    for (Map.Entry<String, QueryStruct> termEntry : wordsInDoc.entrySet()) {
        String word = termEntry.getKey();
        if (!"--maxFreq--".equals(word)) {
            double termFreq = termEntry.getValue().ratioOverall;
            double tf = alpha + (1-alpha) * termFreq / maxFreq;
            wordsInDoc.get(word).ratioOverall = tf;
        }
    }
}

//calculate idf
double totalDocuments = tfMap.size();
for(String word : words) {
    int containingDocs = 0;
    for(Map<String, QueryStruct> map : tfMap.values()) {
        if(map.containsKey(word)) {
            containingDocs++;
        }
    }
    double idf = 0.1 + Math.log((totalDocuments + 1) / (double)(containingDocs + 1));
    idfMap.put(word, idf);
}

// //calculate weight
Map<String, Double> weightMap = new HashMap<String, Double>();
for (Map.Entry<String, Map<String, QueryStruct>> entry : tfMap.entrySet()) {
    String link = entry.getKey();
    Map<String, QueryStruct> wordsInDoc = entry.getValue();
    
    //
    Row curUrlRow = kvs.getRow("pt-pageranks", link);
    double pr = Double.parseDouble(curUrlRow.get("pagerank"));
    
    double tf_Value = wordsInDoc.get("--maxFreq--").ratioOverall;
    double weight = 0.0;
    for (Map.Entry<String, QueryStruct> termEntry : wordsInDoc.entrySet()) {
        String term = termEntry.getKey();
        if (!"--maxFreq--".equals(term)) {
            double ratioOverTitle = termEntry.getValue().ratioOverTitle;
            weight += termEntry.getValue().ratioOverall * idfMap.get(term) * (ratioOverTitle == 0 ? 1 : (10 * Math.log(ratioOverTitle) + 1));
        }
    }
    double combinedWeight = weight * TF_IDF_WEIGHT + pr * PR_WEIGHT;
    weightMap.put(link, combinedWeight);
}

// Integer k = 10;
// List<Map.Entry<String, Double>> hostTopK = hostMap.entrySet()
// .stream()
// .sorted(Map.Entry.comparingByValue(Comparator.reverseOrder()))
// .limit(k)
// .collect(Collectors.toList());

int k = 1;
List<Map.Entry<String, Double>> topK = weightMap.entrySet()
        .stream()
        .sorted(Map.Entry.comparingByValue(Comparator.reverseOrder()))
        //.limit(k)
        .collect(Collectors.toList());

if(loadingPos >= topK.size()){
    topK = topK.subList(0, topK.size());
}else{
    topK = topK.subList(0, loadingPos);
}

List<String> topKkeys = topK.stream()
        .map(entry -> entry.getKey())
        .collect(Collectors.toList());

// List<String> topKkeys = topK.stream()
//         .map(entry -> entry.getKey() + ":" + entry.getValue())
//         .collect(Collectors.toList());

// List<String> topKhosts = hostTopK.stream()
//         .map(entry -> entry.getKey() + ":" + entry.getValue())
//         .collect(Collectors.toList());

// combine the two lists
List<String> topKlist = new ArrayList<String>();
// topKlist.addAll(topKhosts);
topKlist.addAll(topKkeys);

StringBuilder resString = new StringBuilder();
for(String r : topKlist)
{
    resString.append(r);
    resString.append("|");
    resString.append(link2Title.get(r));
    resString.append("\n");
}

// Row historyCache = new Row(query);
// historyCache.put("history", resString.toString());
// kvs.putRow("pt-searchhistory", historyCache);
// return ret.toString();
return resString.toString();
//return ans;
            }
        });
    }
}
