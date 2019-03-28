package utilby;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import java.io.IOException;
import java.net.URLDecoder;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ExtractUrlFromHtml {

    public static List<String> extractUrlFromHtml(String html, String host,List<Pattern>patterns)
            throws IOException{
        List<String>urlList=new LinkedList<>();

        Document doc = Jsoup.parse(html);
        Elements tags=doc.select("a");

        String hostNo=host.replace("https://","")
                .replace("http://","").trim();

        String http="";
        Pattern hostPattern=Pattern.compile("^(h.*?://)");
        Matcher hostMatcher=hostPattern.matcher(host);
        if(hostMatcher.find()){
            http=hostMatcher.group(1);
        }
        if(tags.size()>0){
            for(Element tag:tags){
                String url=tag.attr("href").trim();

                if(url==null){
                    continue;
                }

                for(Pattern pattern:patterns){
                    Matcher matcher=pattern.matcher(url);

                    int index=0;

                    while (matcher.find() && index<1000){
                        url=matcher.group(1);

                        if (url.contains(host)){
                            urlList.add(URLDecoder.decode(url, "utf8"));
                        }
                        else if (url.contains(hostNo)){
                            url=http+url;
                            urlList.add(URLDecoder.decode(url, "utf8"));
                        }
                        else{
                            url=host+matcher.group();
                            urlList.add(URLDecoder.decode(url, "utf8"));
                        }
                        index++;
                    }

                }
            }
        }

        return urlList;
    }

}
