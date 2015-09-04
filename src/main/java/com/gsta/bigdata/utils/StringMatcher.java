package com.gsta.bigdata.utils;

import java.util.Vector;

/**
 * 
 * @author tianxq
 *
 */
public class StringMatcher {
	protected Vector<String> segments = new Vector<String>();
	
	public StringMatcher(String pattern){
		compile(pattern);
	}
	
	protected void compile(String pattern){
		String segment = "";
		for (int i = 0 ; i < pattern.length() ; i ++){
			if (pattern.charAt(i) == '*'){
				if (segment.length() > 0){
					segments.add(segment);
					segment = "";
				}
				segments.add("*");
			}else{
				segment += pattern.charAt(i);
			}
		}
		if (segment.length() > 0){
			segments.add(segment);
			segment = "";
		}		
	}
	
	public boolean match(String data){
		int current = 0;
		boolean any = false;
		for (String segment:segments){
			if (segment.equals("*")){
				any = true;
				continue;
			}
			int found = data.indexOf(segment,current);
			boolean matched = any ? found >= current : found == current;
			if (!matched){
				return false;
			}			
			current = found + segment.length();
			any = false;
		}
		return true;
	}	

}


