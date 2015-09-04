package com.gsta.bigdata.utils;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
/**
 * 
 * @author tianxq
 *
 */
public class HdfsUtils {
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
}
