package hudongbaike;

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
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class GetUserInfo {
    private static Logger logger = LoggerFactory.getLogger(GetUserInfo.class);
    private static SimpleDateFormat df= new SimpleDateFormat("yyyy-MM-dd HH:mm");
    private static Pattern pattern=Pattern.compile("-->(\\{.*})");
    private static SimpleDateFormat dof=new SimpleDateFormat("EEE MMM dd HH:mm:ss Z yyyy", Locale.UK);


    public static void main(String[] args)
            throws IOException {

        Map<String, String> argMap = utilby.CmdlineParser.parse(args);

        String input = argMap.get("input");
        String output = argMap.get("output");

        if (input == null || output == null ) {
            logger.info("You should specify urls, stats");
            System.exit(1);
        }

        if (!output.endsWith("/")) {
            output = output + "/";
        }


        try{
            FileSystem fs = utilby.HdfsUtil.getDefaultFileSystem();

            List<Path> htmlPathList = utilby.HdfsUtil.getPathList(fs, input);

            //Set<String> results=new HashSet<>();

            DateFormat datetimeFormat = new SimpleDateFormat("yyyyMMddHHmmss");
            String datetimeStr = datetimeFormat.format(new Date());
            String parseArticle= output + "parser" + datetimeStr;

            FSDataOutputStream fow = fs.create(new Path(parseArticle));


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
                    if(!parts[1].startsWith("<!")){
                        continue;
                    }

                    JsonObject userInfo=new JsonObject();
                    logger.info("Process file: {}", parts[1]);
                    String userUrl=getUpateTime.getUrlfromHtml(parts[1]);
                    if (userUrl.equals(null)){
                        continue;
                    }
                    if (!userUrl.contains("http://i.baike.com/")){
                        continue;
                    }

                    String userId=userUrl.replace("&t=0.7325755269688712","")
                            .replace("http://i.baike.com/profile.do?action=more&useriden=","")
                            .trim();

                    Date downLoadTime=dof.parse(new Date(utilby.Stringutil.getHtmlFetchTimeMills(parts[1])).toString());

                    Gson gson =new Gson();
                    Matcher matcher=pattern.matcher(parts[1]);
                    if(matcher.find()){
                        JsonObject jsonOb=gson.fromJson(matcher.group(1), JsonObject.class);
                        int recordCount=0;
                        if (jsonOb.has("feedsDepart")){
                            JsonObject info=jsonOb.getAsJsonObject("feedsDepart");

                            if (info.has("recordCount")){
                                recordCount=Integer.parseInt(info.get("recordCount").toString());
                            }

                        }
                        else {
                            continue;
                        }
                        if (recordCount==0){
                            continue;
                        }

                        if(jsonOb.has("feeds")){
                            JsonArray feeds=jsonOb.getAsJsonArray("feeds");
                            if (feeds.size()>0){
                                Object feedFirst=feeds.get(0);
                                Object feedLast=feeds.get(feeds.size()-1);
                                if(feedFirst instanceof JsonObject && feedLast instanceof JsonObject){

                                    String time1=((JsonObject) feedFirst).get("feedTime").toString()
                                            .replace("\"","").trim();
                                    Date timeFirst=getUpateTime.getTimefromString(time1,downLoadTime);


                                    String time2=((JsonObject) feedLast).get("feedTime").toString()
                                            .replace("\"","").trim();
                                    Date timeLast=getUpateTime.getTimefromString(time2,downLoadTime);

                                    double speed=0.00;

                                    long diff = (downLoadTime.getTime() - timeLast.getTime())/1000;
                                    if(diff==0){
                                        speed=0.00;
                                    }
                                    else{
                                        speed=((double)diff)/((double)recordCount);
                                    }

                                    userInfo.addProperty("UserId",userId);
                                    userInfo.addProperty("RecordCount",recordCount);
                                    userInfo.addProperty("LastUpdateTime",timeFirst.toString());
                                    userInfo.addProperty("UpdateSpeed(diff/recordCount)",speed);
                                    userInfo.addProperty("DownTime",downLoadTime.toString());
                                }

                            }
                        }
                        line = userInfo.toString() + "\n";
                        logger.info(line);
                        fow.write(line.getBytes("UTF-8"));

                    }
                }
            }
            fow.close();

        }catch (Exception e) {
            e.printStackTrace();
        }

    }
}
