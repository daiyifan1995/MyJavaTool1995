package qqmusic;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;


public class GetSingerId {
    private static Logger logger = LoggerFactory.getLogger(GetSingerId .class);

    public static void main(String[] args)
            throws IOException {

        Map<String, String> argMap = utilby.CmdlineParser.parse(args);

        String input = argMap.get("input1");
        String inputUrl = argMap.get("inputurl");
        String output = argMap.get("output");


        if (inputUrl == null || output == null || input==null ) {
            logger.info("You should specify input,inputUrl and outputDir");
            System.exit(1);
        }

        if (!output.endsWith("/")) {
            output = output + "/";
        }

        try{
            FileSystem fs = utilby.HdfsUtil.getDefaultFileSystem();

            Set<String> urlCrawled=new HashSet<>();
            List<Path> htmlCrawledPathList = utilby.HdfsUtil.getPathList(fs, inputUrl);

            for (org.apache.hadoop.fs.Path Path: htmlCrawledPathList ){
                logger.info("Process file: {}", Path);
                FSDataInputStream fis = fs.open(Path);
                BufferedReader br = new BufferedReader(new InputStreamReader(fis));
                while(true){
                    String line = br.readLine();//文件较大，内存不够，因此每次只读取一行
                    if (line == null) {
                        break;
                    }
                    String url=line.replace("http://","")
                            .replace("\n","").trim();
                    logger.info("crawled url: {}", url);
                    urlCrawled.add(url);
                }
            }


            List<Path> htmlPathList = utilby.HdfsUtil.getPathList(fs, input);
            Set<String> idList=new HashSet<>();

            for (org.apache.hadoop.fs.Path Path: htmlPathList){
                logger.info("Process file: {}", Path);
                FSDataInputStream fis = fs.open(Path);
                BufferedReader br = new BufferedReader(new InputStreamReader(fis));
                while(true) {
                    String line = br.readLine();//文件较大，内存不够，因此每次只读取一行
                    if (line == null) {
                        break;
                    }

                    Gson gson=new Gson();
                    JsonObject jsonObj=gson.fromJson(line, JsonObject.class);
                    if(jsonObj.has("singer")){
                        JsonArray singers=jsonObj.getAsJsonArray("singer");//若该singer的key找不到会报错
                        logger.info("line singers{} ",singers.toString());
                        if(singers.size()>0){
                            for (Object singer : singers) {
                                if(singer instanceof JsonObject)
                                {
                                    String url=((JsonObject) singer).get("singerUrl").toString();
                                    logger.info("url {}",url);
                                    url=url.replace("https://","")
                                            .replace("\"","")
                                            .replace("\n","")
                                            .trim();
                                    logger.info("url {}",url);
                                    if(urlCrawled.contains(url)){
                                        logger.info("urlcrawled {}",url);
                                        continue;
                                    }
                                    else{
                                        String singerId=((JsonObject) singer).get("singerId").toString().replace("\"","");
                                        String pre="https://y.qq.com/n/m/detail/singer/index.html?ADTAG=newyqq.singer&source=ydetail&singerId=";
                                        singerId=pre.concat(singerId);
                                        logger.info("urlcrawled size {} url {} idList {}",urlCrawled.size(),url,singerId);
                                        idList.add(singerId);
                                    }
                                }
                            }
                        }
                    }

                }
                br.close();
                fis.close();
            }
            DateFormat datetimeFormat = new SimpleDateFormat("yyyyMMddHHmmss");
            String datetimeStr = datetimeFormat.format(new Date());
            String parseSinger= output + "uncrawledSingerUrl" + datetimeStr;

            FSDataOutputStream fow = fs.create(new Path(parseSinger));

            utilby.HdfsUtil.writeLineList(idList, fs, new Path(parseSinger), false);

            fow.close();

        }catch (Exception e) {
            e.printStackTrace();
        }

    }



}


