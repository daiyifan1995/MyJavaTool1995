package utilby;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.CompressionInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.util.*;

public class HdfsUtil {
	
	public static final String PATH_SEPERATOR = ";";

	private static final Logger logger = LoggerFactory.getLogger(HdfsUtil.class);
	
	public static List<Path> getPathList(FileSystem fs, String pathStr) {
		List<FileStatus> stats = new LinkedList<>();
		String[] parts = pathStr.split(PATH_SEPERATOR);
		for (String part: parts) {
			part = part.trim();
			if (part.isEmpty()) {
				continue;
			}
			Path path = new Path(part);
			try {
				if (part.indexOf("*") > 0) {
					stats.addAll(Arrays.asList(fs.globStatus(path)));
				}else /* if (part.endsWith("/")) */ {
					stats.addAll(Arrays.asList(fs.listStatus(path)));
				}
			}catch (Exception e) {
				logger.error("Path element error ", e);
				return null;
			}
		}
		Collections.sort(stats, new Comparator<FileStatus>() {
			@Override
			public int compare(FileStatus o1, FileStatus o2) {
				 if (o1.getModificationTime() > o2.getModificationTime()) {
					 return 1;
				 }
				 if (o1.getModificationTime() < o2.getModificationTime()) {
					 return -1;
				 }
				 return 0;
			}
		});
		List<Path> retList = new ArrayList<Path>();
		for (FileStatus stat: stats) {
			retList.add(stat.getPath());
		}
		return retList;
	}
	
	public static List<Path> getPathList(String pathStr) {
		return getPathList(getDefaultFileSystem(), pathStr);
	}
	
	public static List<String> getLineList(FileSystem fs, Path path) throws Exception {
		FSDataInputStream fis = fs.open(path);
		Configuration conf = new Configuration();
		CompressionCodecFactory factory = new CompressionCodecFactory(conf);
		
		CompressionCodec codecInput = factory.getCodec(path);
		BufferedReader br = null;
		if (codecInput != null) {
			CompressionInputStream comInputStream = codecInput.createInputStream(fis);
			InputStreamReader fin = new InputStreamReader(comInputStream);
			br =new BufferedReader(fin);
		}else {
			br = new BufferedReader(new InputStreamReader(fis));
		}
		List<String> lineList = new ArrayList<String>();
		while (true) {
			String line = br.readLine();
			if (line == null) {
				break;
			}
			lineList.add(line);
		}
		br.close();
		fis.close();
		return lineList;
	}
	
	public static void writeLineList(Collection<String> lineList, FileSystem fs, Path path, boolean append) throws Exception {
		FSDataOutputStream fos = null;
		if (append) {
			fos = fs.append(path);
		}else {
			fos = fs.create(path);
		}
		PrintWriter pw = new PrintWriter(fos);
		for (String line: lineList) {
			pw.println(line);
		}
		pw.close();
		fos.close();
	}
	
	public static FileSystem getDefaultFileSystem() {
		Configuration conf = new Configuration();
		try {
			return FileSystem.get(conf);
		}catch (Exception e) {
			
			return null;
		}
	}
}
