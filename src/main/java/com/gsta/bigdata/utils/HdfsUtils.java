package com.gsta.bigdata.utils;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
/**
 * 
 * @author tianxq
 *
 */
public class HdfsUtils {
	private Logger logger = LoggerFactory.getLogger(getClass());
	
	public InputStream load(URLocation url) throws Exception {
		return load(url.getPath());
	}

	public InputStream load(String path) throws IOException {
		Configuration conf = new Configuration();

		FileSystem fs = FileSystem.get(URI.create(path), conf);
		return fs.open(new Path(path));

	}

	public OutputStream open(String path) throws IOException{
		if(path == null){
			return null;
		}
		
		Path objPath = new Path(path);
		Configuration conf = new Configuration();
		conf.setBoolean("dfs.support.append", true); 

		FileSystem fs = FileSystem.get(URI.create(path),conf);
		if(fs.exists(objPath)){
			return fs.append(objPath);
		}
		return fs.create(objPath);
	}
	
	public void WriteToHDFS(String path,String fileName, String words) throws IOException, URISyntaxException  
    {  
		if(path == null || "".equals(path)){
			return;
		}
		
        Configuration conf = new Configuration();  
        FileSystem fs = FileSystem.get(URI.create(path), conf);  
        Path p = new Path(path); 
        if (fs.exists(p) && fs.delete(p, true)) {
			logger.info("delete  dir=" + p);
		}
        
        String file = path + "/" + fileName; 
        p = new Path(file);
        FSDataOutputStream out = fs.create(p);   
        out.writeBytes(words);    
        out.write(words.getBytes("UTF-8"));  
          
        out.close();  
        fs.close();
    }  
	
	public String readFromHDFS(String file) throws IOException {
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(file), conf);
		Path path = new Path(file);
		FSDataInputStream in = fs.open(path);
		
		FileStatus stat = fs.getFileStatus(path);
		byte[] buffer = new byte[Integer
				.parseInt(String.valueOf(stat.getLen()))];
		in.readFully(0, buffer);
		in.close();
		fs.close();
		
		return new String(buffer);
	}
	
	public static void main(String[] args){
		HdfsUtils hdfsUtils = new HdfsUtils();
		String path = "hdfs://10.17.35.120:8020/user/tianxq/spark/spark";
		String fileName = "json.test";
		String words = "aaaaaaaaaaaaaaaaa";
		String file = path + "/" + fileName;
		
		try {
			hdfsUtils.WriteToHDFS(path, fileName, words);
			String str = hdfsUtils.readFromHDFS(file);
			System.out.println(str);
		} catch (IOException | URISyntaxException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
