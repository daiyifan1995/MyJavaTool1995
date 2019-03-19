import java.net.URLDecoder;
import java.net.URLEncoder;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StringUtil {

    private static final Logger logger = LoggerFactory.getLogger(StringUtil.class);

    public static String decode(String str) {
        try {
            return URLDecoder.decode(str, "utf-8");
        } catch (Exception e) {
            return str;
        }
    }

    public static String encode(String str) {
        try {
            str = URLEncoder.encode(str, "utf-8");
            str = str.replace("+", "%20");
            return str;
        } catch (Exception e) {
            return str;
        }
    }

    public static Pattern pattern = Pattern.compile("(.+/item/)(.+)(/\\d+)$");
    public static Pattern pattern2 = Pattern.compile("(.+/item/)(.+)$");

    public static String getEncodedBaiduUrl(String url) {
        Matcher matcher = pattern.matcher(url);
        if (!matcher.matches()) {
            matcher = pattern2.matcher(url);
            if (matcher.matches()) {
                String title = matcher.group(2).trim();
                return matcher.group(1) + encode(title);
            }
            return url;
        }
        String title = matcher.group(2).trim();
        return matcher.group(1) + encode(title) + matcher.group(3);
    }

    public static boolean isEmpty(String str) {
        return str == null || str.isEmpty();
    }

    public static String getPath(String url) {
        url = url.replace("https://", "").replace("http://", "");
        int pos = url.indexOf("/");
        if (pos < 0 || pos == url.length() - 1) {
            return "/";
        }else {
            return url.substring(pos + 1);
        }
    }

    public static String getHost(String url) {
        url = url.replace("https://", "").replace("http://", "");
        int pos = url.indexOf("/");
        if (pos < 0) {
            return url;
        }else {
            return url.substring(0, pos);
        }
    }

    public static Map<String, String> getParamMap(String url) {
        int pos = url.indexOf("?");
        if (pos <= 0) {
            return null;
        }

        Map<String, String> retMap = new HashMap<String, String>();
        url = url.substring(pos+1);
        String parts[] = url.split("&");
        for (String part: parts) {
            String keyValue[] = part.split("=");
            if (keyValue.length !=  2) {
                continue;
            }
            retMap.put(keyValue[0], keyValue[1]);
        }
        return retMap;
    }

    static int LEN = "&pn=0".length();
    public static String getUniqBaiduJingyanUrl(String url) {
        url = url.trim();
        url =url.replace("http:", "https:");
        if (url.indexOf("/article/") > 0 && url.indexOf("?") > 0) {
            url = url.substring(0, url.indexOf("?"));
        }else if (url.indexOf("/user/") > 0 && url.endsWith("&pn=0")) {
            url = url.substring(0, url.length() - LEN);
        }
        return url;
    }

    public static long getHtmlFetchTimeMills(String html) {
        html = html.trim();
        if (!html.startsWith("<!--")) {
            return 0;
        }
        int endPos = html.indexOf("-->");
        String info = html.substring("<!--".length(), endPos);
        String[] parts = info.split("\t");
        if (parts.length < 2) {
            return 0;
        }
        long fetchMills = Long.parseLong(parts[1].trim());
        return fetchMills;
    }

    /*
     *   统一换成  iso 格式的 日期时间， 好处是 一目了然
     **/
    public static String getHtmlFetchTimeStr(String html) {
        long fetchMills = getHtmlFetchTimeMills(html);
        if (fetchMills <= 0) {
            return "";
        }

        return DateUtil.toISODateTimeString(new Date(fetchMills));
    }

    public static void main(String[] args) {
//		List<String> lineList = FileUtils.getFileLines("item_20.txt", "utf-8");
//		for (String line: lineList) {
//			int pos = line.indexOf("/item/");
//			String part = line.substring(pos + "/item/".length());
//			System.out.println(encode(part));
//		}
        //System.out.println(decode("https://baike.baidu.com/item/100%E7%A7%8D%E7%89%B9%E6%95%88%E8%AE%BE%E8%AE%A1photoshop+CS%E5%B9%B3%E9%9D%A2%E8%AE%BE%E8%AE%A1%E5%AE%9E%E4%BE%8B%E6%89%8B%E5%86%8C/6386758"));
        System.out.println(getPath("https://baike.baidu.com/item/Why?中国青少年动漫大百科"));
        System.out.println(getPath("https://baike.baidu.com/"));
        System.out.println(getPath("https://baike.baidu.com"));
    }
}
