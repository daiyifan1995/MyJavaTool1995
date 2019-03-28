package utilby;

import java.util.HashMap;
import java.util.Map;

public class CmdlineParser {
	
	public static Map<String, String> parse(String[] args){
		Map<String, String> retMap = new HashMap<String, String>();
		for (int i = 0; i <args.length; i++) {
			String arg = args[i];
			if (arg.startsWith("--") && i < args.length - 1) {
				i++;
				retMap.put(arg.substring(2), args[i]);
			}
		}
		return retMap;
	}
}
