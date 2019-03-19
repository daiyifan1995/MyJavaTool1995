package baidujingyan;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class BaiduJingyanHtmlToJsonParser {
    
    private static Logger logger = LoggerFactory.getLogger(BaiduJingyanHtmlToJsonParser.class);
    
    public static boolean extractArticle(String html, String url, JsonObject jsonObj){
        //Map<String, Object> map = new HashMap<String, Object>();
        Pattern pattern = Pattern.compile("<.*?>");
        Document doc = null;
        try{
            doc = Jsoup.parse(html);
            String title = doc.title()
                    .replace("_百度经验", "");
            jsonObj.addProperty("title",title);
        } catch (Throwable e){
            logger.error("Title Error: {}", url, e);
            return false;
        }

        try{
            String path= getPath(doc,pattern);
            jsonObj.addProperty("path",path);
        } catch (Throwable e){
            logger.error("Path Error: {}", url, e);
        }

        try{
            String scans=doc.select("ul[class='exp-info']").
                    select("span[class='views']").text();

            String time=doc.select("ul[class='exp-info']")
                    .select("time").text();
            if(scans.equals("") && time.equals("")){
                jsonObj.add("Video", getVideos(doc));
            }
            else{
                jsonObj.addProperty("scans",scans);
                jsonObj.addProperty("time",time);
            }

        }
        catch (Throwable e){
            logger.error("Video Error: {}", url, e);
        }

        try{
            String summary=doc.select("div[class='content-listblock-text']")
                    .toString().trim()
                    .replace("</p>","\u0001").replaceAll("&nbsp|;&gt;", "")
                    .replaceAll("<.*?>","")
                    .replace("&nbsp","")
                    .trim()
                    .replace("\n","\u0001");
            jsonObj.addProperty("summary",summary);
        } catch (Throwable e){
            logger.error("Summary Error: {}", url, e);
        }

        try{
            JsonArray content = getContent(doc,url);
            jsonObj.add("content",content);
        } catch (Throwable e){
            logger.error("Content Error: {}", url, e);
        }

        try{
            String likes=doc.select("span[class='like-num']").text();
            jsonObj.addProperty("likes",likes);//点踩数没有
        } catch (Throwable e){
            logger.error("Likes Error: {}", url, e);
        }

        try{
            String votes=doc.select("span[id='v-a-num']").text();
            jsonObj.addProperty("votes",votes);
        } catch (Throwable e){
            logger.error("Votes Error: {}", url, e);
        }
        jsonObj.addProperty("url",url);

        return true;
    }

    public static String getPath(Document doc, Pattern pattern)
            throws IOException{

        String path = doc.select("div[class='bread-wrap']").first().toString();

        Matcher matcher = pattern.matcher(path);

        while (matcher.find()) {
            String m = matcher.group();
            path = path.replace(matcher.group(), ">");
        }

        path = path.trim().replaceAll("\\s*", "")
                .replaceAll("百度经验", "")
                .replaceAll("&nbsp|;&gt;", "")
                .replaceAll(">>", "")
                .replaceAll("\r", "").replaceAll("&nbsp|;&gt;", "");

        if (path.substring(path.length() - 1).equals(">")) {
            path = path.substring(0, path.length() - 1);
        }

        if (path.substring(0, 1).equals(">")) {
            path = path.substring(1, path.length());
        }

        return path;
    }

    public static JsonArray getContent(Document doc, String url)
            throws IOException{

        Pattern pattern=Pattern.compile("data-src=\"(.*?)\"");

        JsonArray contents = new JsonArray();

        Elements modules=doc.select("div[class='exp-content-block']");

        for(Element module:modules){
            JsonObject content=new JsonObject();

            String head=module.select("h2[class='exp-content-head']").text();

            if(head.equals("")){
                continue;
            }

            content.addProperty("head",head);


            Elements elements=module.select("li");

            JsonArray  steps=new JsonArray();
            if (elements.size()!=0){

                for(Element element :elements) {

                    JsonObject step=new JsonObject();

                    String order=element.attr("class")
                            .replace("exp-content-list list-item-","");
                    String text;
                    try{
                        text=element.select("div[class='content-list-text']")
                                .toString().trim()
                                .replace("<br>|</p>","\u0001")
                                .replaceAll("步骤阅读|<.*?>","")
                                .replace("&nbsp","")
                                .replaceAll("&nbsp|;&gt;", "")
                                .trim()
                                .replace("\n","\u0001");
                    }
                    catch (NullPointerException e){
                        text="";
                    }
                    step.addProperty("text",text);
                    step.addProperty("step",order);


                    String imgs=element.select("img").toString();
                    List<String> imglist=new LinkedList<>();
                    Matcher matcher=pattern.matcher(imgs);
                    while (matcher.find()) {
                        imglist.add(matcher.group(1));
                    }
                    if(imglist.size()!=0) {
                        JsonArray jsonArr = new JsonArray();
                        for (String img: imglist) {
                            jsonArr.add(new JsonPrimitive(img));
                        }
                        step.add("jpg",jsonArr);
                    }

                    if(!step.toString().equals("{}"))
                        steps.add(step);

                }
            }
            else{
                elements=module.select("div[class='exp-content-listblock']");
                if (elements.size()!=0){
                    Integer order=0;
                    for(Element element :elements){

                        JsonObject step=new JsonObject();

                        String text;

                        try{
                            text=element.select("div[class='content-listblock-text']")
                                    .toString()
                                    .trim()
                                    .replace("<br>|</p>","\u0001")
                                    .replaceAll("步骤阅读|<.*?>","")
                                    .replace("&nbsp","")
                                    .replaceAll("&nbsp|;&gt;", "")
                                    .trim()
                                    .replace("\n","\u0001");
                        }
                        catch (NullPointerException e){
                            text="";
                        }
                        order++;
                        step.addProperty("text",text);
                        step.addProperty("step",order);


                        String imgs=element.select("img").toString();
                        List<String> imglist=new LinkedList<>();
                        Matcher matcher=pattern.matcher(imgs);
                        while (matcher.find()) {
                            imglist.add(matcher.group(1));
                        }
                        if(imglist.size()!=0) {
                            JsonArray jsonArr = new JsonArray();
                            for (String img: imglist) {
                                jsonArr.add(new JsonPrimitive(img));
                            }
                            step.add("jpg",jsonArr);
                        }

                        if(!step.toString().equals("{}"))
                            steps.add(step);

                    }
                }
            }


            if(steps.size()!=0){
                content.add("steps",steps);
            }

            contents.add(content);
        }

        return contents;
    }

    public static JsonObject getVideos(Document doc)
            throws IOException {

        JsonObject video = new JsonObject();

        String Vid = doc.select("li[class='pt-10 pb-10 feeds-video-list-item feeds-video-list-item-0 clearfix current']")
                .attr("vid");

        String x=String.format("!function\\(\\).*videoList:\\[(\\{\"feedVid\":\"%s.*?}).*?\\].*function\\(\\)",Vid);
        Pattern p=Pattern.compile(x);
        Matcher m = p.matcher(doc.toString().replaceAll("\\s*", ""));
        while (m.find()) {
            String map = m.group(1);
            for (String i : map.split(",")) {
                if (i.startsWith("\"playUrl\"")) {
                    String videoSrc = i.replace("\"playUrl\":\"", "");
//                            .replace("\"", "")
//                            .replace("\\", "");
                    video.addProperty("VideoSrc", videoSrc);
                }
                if (i.startsWith("\"videoView\"")) {
                    String views = i.replace("\"videoView\":", "")
                            .replace("}", "");
                    video.addProperty("views", views);
                }
            }
        }
        return video;
    }
    
    public static boolean extractUser(String html, String url, JsonObject jsonObj){
        //
        Document doc = null;
        //Map<String,Object> map = new HashMap<String, Object>();
        
        String uid = getUidFromUrl(url);
        if (uid == null || uid.isEmpty()) {
            return false;
        }

        jsonObj.addProperty("url",url);
        try{
            doc = Jsoup.parse(html);
            String userName = doc.select("a[class='uname']").text();
            jsonObj.addProperty("userName",userName);
        } catch (Throwable e){
            logger.error("Title Error: {}", url, e);
            return false;
        }

        try{
            Elements elements = doc.select("span[class='label']");
            for(Element element:elements){
                String item=element.text();
                Element next=element.nextElementSibling();
                if(next.attr("class").equals("score") ||
                        next.attr("class").equals("f-666") ){
                    String value=next.text();
                    jsonObj.addProperty(item,value);
                }
            }
        } catch (Throwable e){
            logger.error("infomation Error: {}", url, e);
        }

        try{
            Elements elements = doc.select("p[class='data-tit']");
            for(Element element:elements){
                String item=element.text();
                Element next=element.previousElementSibling();
                if(next.attr("class").equals("data-num")  ){
                    String value=next.text();
                    jsonObj.addProperty(item,value);
                }
            }
        } catch (Throwable e){
            logger.error("infomation Error: {}", url, e);
        }


        return true;
    }
    
    public static String getUidFromUrl(String url) {
        int pos = url.indexOf("?");
        if (pos <= 0) {
            return null;
        }
        
        url = url.substring(pos+1);
        String parts[] = url.split("&");
        for (String part: parts) {
            String keyValue[] = part.split("=");
            if (keyValue.length !=  2) {
                continue;
            }
            if (keyValue[0].equals("uid")) {
                return keyValue[1].trim();
            }
        }
        return null;
    }
}
