
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DateUtil {

    private static final Logger logger = LoggerFactory.getLogger(DateUtil.class);

    public static String toDateString(Date date) {
        DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
        return dateFormat.format(date);
    }

    public static Date parseDate(String dateStr) {
        try {
            DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
            return dateFormat.parse(dateStr);
        } catch (Exception e) {
            return null;
        }
    }

    public static String toDateTimeString(Date datetime) {
        DateFormat datetimeFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        return datetimeFormat.format(datetime);
    }

    public static Date parseDateTime(String dateTimeStr) {
        try {
            DateFormat datetimeFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            return datetimeFormat.parse(dateTimeStr);
        } catch (Exception e) {
            return null;
        }
    }

    public static Date parseISODateTime(String dateTimeStr) {
        try {
            DateFormat isoFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssXXX");
            return isoFormat.parse(dateTimeStr);
        } catch (Exception e) {
            return null;
        }
    }

    public static String toISODateTimeString(Date date) {
        try {
            DateFormat isoFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssXXX");
            return isoFormat.format(date);
        }catch (Exception e){
            logger.error("Exception occur, date:{}", date, e);
            return "";
        }
    }

    public static void main(String[] args) {
        String dateStr = "2018-12-20 00:12:14";
        Date date = parseDateTime(dateStr);
        System.out.println(toISODateTimeString(date));

    }
}

