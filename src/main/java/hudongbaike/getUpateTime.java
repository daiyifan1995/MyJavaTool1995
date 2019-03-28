package hudongbaike;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.regex.Pattern;


public class getUpateTime {
    private static SimpleDateFormat df= new SimpleDateFormat("yyyy-MM-dd HH:mm");
    private static long oneDayMills=86400000;
    private static long oneHourMills=3600000;
    private static long oneMinMills=60000;
    private static long onessMills=1000;
    Pattern pattern=Pattern.compile("\"recordCount\":0");

    public static Date getTimefromString(String time,Date downLoadTime)
            throws Exception {
        Date timeDate=new Date(0);

        if (time.contains("天前")){
            double beforeday;
            try{
                beforeday=Double.parseDouble(time.replaceAll("天前.*","").trim());
            }
            catch (Throwable e){
                beforeday=0.5;
            }

            long inteval=(long)(downLoadTime.getTime()-oneDayMills*beforeday);
            if(inteval>=0){

                timeDate=new Date(inteval);
            }
        }
        else if(time.contains("小时前")){
            double beforeHour;
            try{
                beforeHour=Double.parseDouble(time.replaceAll("小时前.*","").trim());
            }
            catch (Throwable e){
                beforeHour=0.5;
            }

            long inteval=(long)(downLoadTime.getTime()-oneHourMills*beforeHour);
            if(inteval>=0){
                timeDate=new Date(inteval);
            }

        }
        else if(time.contains("分钟前")){
            double  beforeMin;
            try{
                beforeMin=Double.parseDouble(time.replaceAll("分钟前.*","").trim());
            }
            catch (Throwable e){
                beforeMin=0.5;
            }
            long inteval=(long)(downLoadTime.getTime()-oneMinMills*beforeMin);
            if(inteval>=0){
                timeDate=new Date(inteval);
            }

        }
        else if(time.contains("秒前")){
            double beforess;
            try{
                beforess=Integer.parseInt(time.replaceAll("秒前.*","").trim());

            }
            catch (Throwable e){
                beforess=0.5;
            }
            long inteval=(long)(downLoadTime.getTime()-onessMills*beforess);
            if(inteval>=0){
                timeDate=new Date(inteval);
            }

        }
        else{
            timeDate=df.parse(time);
        }
        return timeDate;
    }

    public static String getUrlfromHtml(String html){
        //<!-- http://i.baike.com/profile.do?action=more&useriden=RAwlxYAxgcnMCeA0B&t=0.7325755269688712       1553655038999 -->{
        html = html.trim();
        if (!html.startsWith("<!--")) {
            return null;
        }
        int endPos = html.indexOf("-->");
        String info = html.substring("<!--".length(), endPos);
        String[] parts = info.split("\t");
        if (parts.length < 2) {
            return null;
        }
        String url=parts[0].trim();

        return url;

    }}
