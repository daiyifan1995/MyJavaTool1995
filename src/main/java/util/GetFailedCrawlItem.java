package util;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;


public class GetFailedCrawlItem {
	
	private static Logger logger = LoggerFactory.getLogger(GetFailedCrawlItem.class);

	public static void main(String[] args) {
		
		Map<String, String> argMap = CmdlineParser.parse(args);
				
		//logger.info("input args: {}, argMap:{}", String.join(" ", args), argMap);
		String urls = argMap.get("urls");
		String stats = argMap.get("stats");
		String outputDir = argMap.get("outputDir");
		
		
		if (urls == null || stats == null || outputDir == null) {
			logger.info("You should specify urls, stats and outputDir");
			System.exit(1);
		}
		
		if (!outputDir.endsWith("/")) {
			outputDir = outputDir + "/";
		}

			
		try {
			FileSystem fs = HdfsUtil.getDefaultFileSystem();
			List<Path> urlPathList = HdfsUtil.getPathList(fs, urls);
			List<Path> statsPathList = HdfsUtil.getPathList(fs, stats);
			
			Set<String> urlSet = new HashSet<String>();
			for (Path urlPath: urlPathList) {
				List<String> lineList = HdfsUtil.getLineList(fs, urlPath);
				logger.info("Path:{}, line size:{}", urlPath, lineList.size());
				for (String line: lineList) {
					if (line.indexOf("comhttp") > 0) {
						logger.info("Bad url:{}", line);
						continue;
					}

					if (!urlSet.add(line) ) {
						logger.info("Duplicate url:{}", line);
					}
				}
				logger.info("Path:{}, size:{}", urlPath, urlSet.size());
				//
			}
			
			Set<String> crawledSet = new HashSet<String>();
			Set<String> failedSet = new HashSet<String>();
			for (Path statsPath: statsPathList) {
				List<String> lineList = HdfsUtil.getLineList(fs, statsPath);
				for (String line: lineList) {
					if (line.isEmpty()) {
						continue;
					}
					String [] parts = line.split("\t");
					if (parts.length < 2) {
						logger.info("bad line:{}", line);
						continue;
					}
					if (parts[1].equals("true")) {
						crawledSet.add(parts[0]);
					}else if (parts[1].equals("false")) {
						failedSet.add(parts[0]);
					}
				}
				logger.info("Path:{}, crawled size:{}, failed size:{}", statsPath, crawledSet.size(), failedSet.size());
			}
			
			logger.info("Before, full: {}, crawled:{}, failed:{}", urlSet.size(), crawledSet.size(), failedSet.size());
			DateFormat datetimeFormat = new SimpleDateFormat("yyyyMMddHHmmss");
			String datetimeStr = datetimeFormat.format(new Date());
			String notCrawledPath = outputDir + "notcrawled" + datetimeStr;
			String failedPath = outputDir + "failed" + datetimeStr;
			String crawledPath = outputDir + "crawled" + datetimeStr;
			
			HdfsUtil.writeLineList(crawledSet, fs, new Path(crawledPath), false);
			
			failedSet.removeAll(crawledSet);
			urlSet.removeAll(crawledSet);
			logger.info("After, not crawled:{}, failed:{}", urlSet.size(), failedSet.size());
				
			HdfsUtil.writeLineList(urlSet, fs, new Path(notCrawledPath), false);
			HdfsUtil.writeLineList(failedSet, fs, new Path(failedPath), false);
			logger.info("Output path， notcrawled:{}, failed:{}", notCrawledPath, failedPath);


		}catch (Exception e) {
			logger.error("Failed", e);
			e.printStackTrace();
		}
	}
}
