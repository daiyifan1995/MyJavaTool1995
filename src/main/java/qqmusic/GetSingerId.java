package qqmusic;

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
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class GetSingerId {
    private static Logger logger = LoggerFactory.getLogger(GetSingerId.class);

    public static void main(String[] args)
            throws IOException {

        Map<String, String> argMap = util.CmdlineParser.parse(args);

        String input = argMap.get("input");
        String output = argMap.get("output");


        if (input == null || output == null ) {
            logger.info("You should specify urls, outputDir");
            System.exit(1);
        }

        if (!output.endsWith("/")) {
            output = output + "/";
        }


        try{
            FileSystem fs = util.HdfsUtil.getDefaultFileSystem();

            List<Path> htmlPathList = util.HdfsUtil.getPathList(fs, input);

            Set<String> urlList=new HashSet<>();



            Pattern singrtUrlPattern=Pattern.compile("https://y.qq.com/n/yqq/singer/(.*?)\\.html");

            for (org.apache.hadoop.fs.Path Path: htmlPathList){
                logger.info("Process file: {}", Path);
                FSDataInputStream fis = fs.open(Path);
                BufferedReader br = new BufferedReader(new InputStreamReader(fis));
                while(true) {
                    String line = br.readLine();//文件较大，内存不够，因此每次只读取一行
                    if (line == null) {
                        break;
                    }
                    logger.info("line Url {} ",line);
                    Matcher singerUrlMatcher=singrtUrlPattern.matcher(line);
                    if(singerUrlMatcher.find()) {
                        String url=singerUrlMatcher.group(1);
                        urlList.add(url);
                        logger.info("Siner Url {} ",url);
                    }

                }
                br.close();
                fis.close();
            }
            DateFormat datetimeFormat = new SimpleDateFormat("yyyyMMddHHmmss");
            String datetimeStr = datetimeFormat.format(new Date());
            String parseSinger= output + "SingerUrl" + datetimeStr;

            FSDataOutputStream fow = fs.create(new Path(parseSinger));

            util.HdfsUtil.writeLineList(urlList, fs, new Path(parseSinger), false);

            fow.close();

        }catch (Exception e) {
            e.printStackTrace();
        }

    }



}


