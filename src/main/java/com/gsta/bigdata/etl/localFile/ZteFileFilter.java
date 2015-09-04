package com.gsta.bigdata.etl.localFile;

import java.io.File;
import java.io.FilenameFilter;

/**
 * 
 * @author tianxq
 *
 */
public class ZteFileFilter implements FilenameFilter {
	private String extension = ".";
	/**
	 * like this:CELLERABADDHO,CELLERABSTAT,CELLERABTIME,CELLPL,CELLRRCCONN
	 * file name must contains one of patterns
	 */
	private String[] patterns = new String[0];
	
	public ZteFileFilter(String extensionNotDot) {
		this.extension = this.extension + extensionNotDot;
	}
	
	public ZteFileFilter(String extensionNotDot,String includeStr) {
		this.extension = this.extension + extensionNotDot;
		if(includeStr != null){
			this.patterns = includeStr.split(",");
		}
	}
	
	@Override
	public boolean accept(File dir, String name) {
		boolean rightExtension =  true;
		if(this.extension != null){
			rightExtension = name.endsWith(extension);
		}
		
		boolean hasInclude = true;
		if(this.patterns != null && this.patterns.length > 0){
			boolean flag = false;
			for(String pattern:this.patterns){
				if(name.endsWith(pattern + this.extension)){
					flag = true;
					break;
				}
			}
			
			hasInclude = flag;
		}
		
		return rightExtension && hasInclude;
	}
}
