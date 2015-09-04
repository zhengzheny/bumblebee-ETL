package com.gsta.bigdata.etl.localFile;

import java.io.File;
import java.io.FilenameFilter;
/**
 * file filter by extension
 * 
 * @author tianxq
 *
 */
public class ExtensionFileFilter implements FilenameFilter{
	private String extension = ".";
	
	public ExtensionFileFilter(String extensionNotDot) {
		this.extension = this.extension + extensionNotDot;
	}
	
	@Override
	public boolean accept(File dir, String name) {
		boolean rightExtension =  true;
		if(this.extension != null){
			rightExtension = name.endsWith(extension);
		}
		
		
		return rightExtension;
	}
}
