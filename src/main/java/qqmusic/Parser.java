package qqmusic;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Map;

public class Parser {

    private static Logger logger = LoggerFactory.getLogger(Parser.class);

    public static void main(String[] args)
            throws IOException {

        Map<String, String> argMap = utilby.CmdlineParser.parse(args);

        String input = argMap.get("input");
        String output = argMap.get("output");
        String outputOtherUrl = argMap.get("output2");

        if (input == null || output == null ) {
            logger.info("You should specify urls, stats and outputDir");
            System.exit(1);
        }

        if (!output.endsWith("/")) {
            output = output + "/";
        }

        if (!outputOtherUrl.endsWith("/")) {
            outputOtherUrl = outputOtherUrl + "/";
        }

        try{
            FileSystem fs = utilby.HdfsUtil.getDefaultFileSystem();

            List<Path> htmlPathList = utilby.HdfsUtil.getPathList(fs, input);

            //Set<String> results=new HashSet<>();

            DateFormat datetimeFormat = new SimpleDateFormat("yyyyMMddHHmmss");
            String datetimeStr = datetimeFormat.format(new Date());
            String parseArticle= output + "parser" + datetimeStr;

            FSDataOutputStream fow = fs.create(new Path(parseArticle));

            String OtherUrl=outputOtherUrl+"OtherUrl"+datetimeStr;
            FSDataOutputStream fowOtherUrl = fs.create(new Path(OtherUrl));

            for (org.apache.hadoop.fs.Path Path: htmlPathList){
                logger.info("Process file: {}", Path);
                FSDataInputStream fis = fs.open(Path);
                BufferedReader br = new BufferedReader(new InputStreamReader(fis));

                while(true){
                    String line = br.readLine();//文件较大，内存不够，因此每次只读取一行
                    if (line == null) {
                        break;
                    }
                    String parts[] = line.split("\t", 2);//html以|t进行分割
                    if (parts.length < 2) {
                        continue;
                    }

                    String html=parts[1];

                    String url=parts[0].trim();
                    if(!url.startsWith("http")){
                        url="https:"+url;

                    }
                    JsonObject jsonObject=new JsonObject();
                    if(url.contains("album")){
                        jsonObject= Parser.parseAlbum(url,html);

                    }

                    else{
                        line = url+ "\t"+parts[1]+"\n";
                        logger.info("other url: {}", url);
                        fowOtherUrl.write(line.getBytes("UTF-8"));
                        continue;
                    }



                    line = jsonObject.toString() + "\n";
                    logger.info(line);
                    fow.write(line.getBytes("UTF-8"));//每个结果依次写；

                }
            }
            fow.close();
            fowOtherUrl.close();
        }catch (Exception e) {
            e.printStackTrace();
        }

    }

    public static JsonObject parseAlbum(String url, String html){
        Document doc= Jsoup.parse(html);
        JsonObject jsonObject=new JsonObject();

        jsonObject.addProperty("url",url);

        Elements titleElements=doc.select("div[class='data__name']");
        if (titleElements.size()>0){
            String title=titleElements.text();
            jsonObject.addProperty("title",title);
        }


        Elements imgElements=doc.select("img[class='data__photo']");
        if (imgElements.size()>0){

            String coverImg=imgElements.get(0).attr("src");
            if (!coverImg.startsWith("https:")){
                coverImg="https:"+coverImg;
            }
            jsonObject.addProperty("coverImg",coverImg);
        }

        Elements singerElements=doc.select("div[class='data__singer']");
        if (singerElements.size()>0){
            JsonArray singers=new JsonArray();
            Elements singeritems=singerElements.get(0).select("a[href^=//y.qq.com/n/yqq/singer/]");
            if(singeritems.size()>0){
                for(Element singeritem:singeritems){
                    JsonObject singer=new JsonObject();
                    String singerName=singeritem.text().trim();
                    String singerUrl=singeritem.attr("href");
                    if (!singerUrl.startsWith("http")) {
                        singerUrl="https:"+singerUrl;
                    }
                    singer.addProperty("singerName",singerName);
                    singer.addProperty("singerUrl",singerUrl);
                    singers.add(singer);
                }
            }

            jsonObject.add("singer",singers);//添加的为string则是addproperty，否则直接为add
        }

        Elements infoElements=doc.select("li[class^=data_info]");
        if (infoElements.size()>0){
            JsonArray infos=new JsonArray();
            JsonObject info=new JsonObject();
            for(Element infoElement:infoElements){
                String item=infoElement.toString().replaceAll("<.*?>","");
                String parts[]=item.split("：",2);
                if(parts.length<2){
                    continue;
                }
                else{

                    Elements hrefs=infoElement.select("a");
                    if(hrefs.size()>0) {
                        JsonObject link=new JsonObject();
                        String infoUrl=hrefs.get(0).attr("href");
                        if (!infoUrl.startsWith("https:")){
                            infoUrl="https:"+infoUrl;
                        }
                        link.addProperty(parts[1],infoUrl);
                        info.add(parts[0],link);
                    }
                    else{
                        info.addProperty(parts[0],parts[1]);
                    }
                    infos.add(info);
                }
            }
            jsonObject.add("information",info);
        }

        Elements songElements=doc.select("span[class^=songlist__songname_txt]");
        if (songElements.size()>0){
            JsonArray songs=new JsonArray();
            for(Element songElement:songElements){
                JsonObject song=new JsonObject();
                String songName=songElement.text();
                String songUrl=songElement.select("a").get(0).attr("href");
                if (!songUrl.startsWith("http")) {
                    songUrl="https:"+songUrl;
                }
                song.addProperty(songName,songUrl);
                songs.add(song);
            }
            jsonObject.add("song",songs);
        }

        Elements summaryElements=doc.select("div[class^=about__cont]");
        if (summaryElements.size()>0){
            JsonArray songs=new JsonArray();
            String summary=summaryElements.get(0).toString().replaceAll("<.*?>","");

            jsonObject.addProperty("summary",summary);
        }


        return jsonObject;

    }

    public static JsonObject parseSong(String url, String html){
        Document doc= Jsoup.parse(html);
        JsonObject jsonObject=new JsonObject();

        jsonObject.addProperty("url",url);

        Elements titleElements=doc.select("div[class='data__name']");
        if (titleElements.size()>0){
            String title=titleElements.text();
            jsonObject.addProperty("title",title);
        }

        Elements imgElements=doc.select("img[class='data__photo']");
        if (imgElements.size()>0){
            String coverImg=imgElements.get(0).attr("src");
            if (!coverImg.startsWith("https:")){
                coverImg="https:"+coverImg;
            }
            jsonObject.addProperty("coverImg",coverImg);
        }

        Elements singerElements=doc.select("div[class='data__singer']");
        if (singerElements.size()>0){
            JsonArray singers=new JsonArray();
            Elements singeritems=singerElements.get(0).select("a[href^=//y.qq.com/n/yqq/singer/]");
            if(singeritems.size()>0){
                for(Element singeritem:singeritems){
                    JsonObject singer=new JsonObject();
                    String singerName=singeritem.text().trim();
                    String singerUrl=singeritem.attr("href");
                    singer.addProperty("singerName",singerName);
                    singer.addProperty("singerUrl",singerUrl);
                    singers.add(singer);
                }
            }

            jsonObject.add("singer",singers);//添加的为string则是addproperty，否则直接为add
        }

        Elements infoElements=doc.select("li[class^=data_info]");
        if (infoElements.size()>0){
            JsonArray infos=new JsonArray();
            JsonObject info=new JsonObject();
            for(Element infoElement:infoElements){
                String item=infoElement.toString().replaceAll("<.*?>","");
                String parts[]=item.split("：",2);
                if(parts.length<2){
                    continue;
                }
                else{

                    Elements hrefs=infoElement.select("a");
                    if(hrefs.size()>0) {
                        JsonObject link=new JsonObject();
                        String infoUrl=hrefs.get(0).attr("href");
                        if (!infoUrl.startsWith("https:")){
                            infoUrl="https:"+infoUrl;
                        }
                        link.addProperty(parts[1],infoUrl);
                        info.add(parts[0],link);
                    }
                    else{
                        info.addProperty(parts[0],parts[1]);
                    }
                    infos.add(info);
                }
            }
            jsonObject.add("information",info);
        }

        Elements summaryElments=doc.select("div[class='about__cont']");
        if(summaryElments.size()>0) {
            String summary=summaryElments.get(0).text();
            jsonObject.addProperty("summary",summary);
        }

        Elements lyricElments=doc.select("div[class='lyric__tit']");
        if(lyricElments.size()>0) {
            String lyric=lyricElments.get(0).text();
            jsonObject.addProperty("lyric",lyric);
        }

        return jsonObject;

    }

    public static JsonObject parseSinger(String url, String html){
        Document doc= Jsoup.parse(html);
        JsonObject jsonObject=new JsonObject();

        jsonObject.addProperty("url",url);

        Elements titleElements=doc.select("div[class='data__name']");
        if (titleElements.size()>0){
            String title=titleElements.text();
            jsonObject.addProperty("title",title);
        }


        Elements imgElements=doc.select("img[class^=data__photo]");
        if (imgElements.size()>0){

            String coverImg=imgElements.get(0).attr("src");
            if (!coverImg.startsWith("https:")){
                coverImg="https:"+coverImg;
            }
            jsonObject.addProperty("coverImg",coverImg);
        }

        Elements singerElements=doc.select("div[class='data__desc_txt']");
        if (singerElements.size()>0){
            JsonObject singer=new JsonObject();
            String singerUrl=singerElements.get(0).text();
            if (!singerUrl.startsWith("https:")){
                singerUrl="https:"+singerUrl;
            }

            jsonObject.add("singer",singer);//添加的为string则是addproperty，否则直接为add
        }

        Elements infoElements=doc.select("li[class^=data_info]");
        if (infoElements.size()>0){
            JsonArray infos=new JsonArray();
            JsonObject info=new JsonObject();
            for(Element infoElement:infoElements){
                String item=infoElement.toString().replaceAll("<.*?>","");
                String parts[]=item.split("：",2);
                if(parts.length<2){
                    continue;
                }
                else{

                    Elements hrefs=infoElement.select("a");
                    if(hrefs.size()>0) {
                        JsonObject link=new JsonObject();
                        String infoUrl=hrefs.get(0).attr("href");
                        if (!infoUrl.startsWith("https:")){
                            infoUrl="https:"+infoUrl;
                        }
                        link.addProperty(parts[1],infoUrl);
                        info.add(parts[0],link);
                    }
                    else{
                        info.addProperty(parts[0],parts[1]);
                    }
                    infos.add(info);
                }
            }
            jsonObject.add("information",info);
        }


        return jsonObject;

    }

}


