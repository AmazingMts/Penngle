package cis5550.frontend;

//keytool -genkeypair -keyalg RSA -alias selfsigned -keystore keystore.jks -storepass secret -dname "CN=Blah, OU=Blubb, O=Foo, L=Bar, C=US"

import static cis5550.webserver.Server.*;

import java.io.File;
import java.net.URLEncoder;
import java.util.*;

import cis5550.tools.HTTP;

public class Frontend {

    public static void main(String[] args) {
        // args[0]: portNum
        // args[1]: queryServerIP
        String queryServerIP = args[1];
        // port(Integer.parseInt(args[0]));
        securePort(Integer.parseInt(args[0]));
        // First use fixed page

        get("/", (req, res)->{
            File mainPageHtml = new File("integrated/page/index.html");
            String mainPageHtmlBuilder = "";
            try{
                mainPageHtmlBuilder = new String(java.nio.file.Files.readAllBytes(mainPageHtml.toPath()));
            }
            catch (Exception e){
                e.printStackTrace();
            }
            res.type("text/html");
            return mainPageHtmlBuilder;
        });

        get("/search", (req, res)->{
            String query = req.queryParams("query");
            StringBuilder ret = new StringBuilder();
            ret.append("<html>")
            .append("<head><title>Search Results</title></head>")
            .append("<body>")
            .append("<h1>Cybersquad Search Engine</h1>")
            .append("<form action='/search' method='get'>")
            .append("<input type='text' name='query' value='").append(query).append("'>")
            .append("<input type='submit' value='Search'>")
            .append("</form>")
            .append("<h2>Search Results</h2>")
            .append("<table border='1'>");
            
            ret.append("<tr>")
            .append("<th>Title</th>")
            .append("<th>URL</th>")
            .append("</tr>");

            List<String> resList = new ArrayList<String>();
            try {
                HTTP.Response response = HTTP.doRequest("GET", "http://" + queryServerIP + "/query?query=" + URLEncoder.encode(query, "UTF-8")+"&pos="+100, null);
                String resListString = new String(response.body());
                resList = Arrays.asList(resListString.split("\n"));
            } catch (Exception e) {
                e.printStackTrace();
            }
            resList.stream().
            map(resString -> splitReturnString(resString)).forEach(resStringArray -> {
                ret.append("<tr>")
                .append("<td>").append(resStringArray[1]).append("</td>")
                .append("<td>").append("<a href='").append(resStringArray[0]).append("'>").append(resStringArray[0]).append("</a>").append("</td>")
                .append("</tr>");
            });

            ret.append("</table>")
            .append("</body>")
            .append("</html>");
            res.type("text/html");
            return ret.toString();
        });

    }

    private static String[] splitReturnString(String returnString){
        System.out.println("returned " + returnString);
        //return new String[]{returnString, returnString};
        int lastColonIndex = returnString.lastIndexOf("|");
        if (lastColonIndex == -1){
            return new String[]{returnString, returnString};
        }
        String title = returnString.substring(0, lastColonIndex);
        String url = returnString.substring(lastColonIndex + 1);
        return new String[]{title, url};
    }
    
}