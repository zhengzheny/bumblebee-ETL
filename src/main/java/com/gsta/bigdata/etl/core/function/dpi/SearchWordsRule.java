package com.gsta.bigdata.etl.core.function.dpi;

import java.io.Serializable;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import com.fasterxml.jackson.annotation.JsonProperty;

public class SearchWordsRule implements Serializable{
	private static final long serialVersionUID = -6979927059699849916L;
	
	@JsonProperty
	private String id = null;  		
	@JsonProperty
	private String charset="utf-8";         
	@JsonProperty
	private String decoder;        
	@JsonProperty
	private int groupIndex;      
	@JsonProperty
	private String regular = null;	
	@JsonProperty
	private Pattern pattern = null;	
	
	
	public SearchWordsRule() {
	}

	public String getRegular() {
		return regular;
	}


	public String getCharset() {
		return charset;
	}

	public void setCharset(String charset) {
		this.charset = charset;
	}

	public String getDecoder() {
		return decoder;
	}

	public void setDecoder(String decoder) {
		this.decoder = decoder;
	}

	public int getGroupIndex() {
		return groupIndex;
	}

	public void setGroupIndex(int groupIndex) {
		this.groupIndex = groupIndex;
	}

	public void setRegular(String regular) {
		this.regular = regular;
		if (regular != null)
		{
			try
			{
				this.pattern = Pattern.compile(regular, Pattern.CASE_INSENSITIVE);
				Matcher matcher = this.pattern.matcher("");
				matcher.find();
			} catch (PatternSyntaxException e)
			{
				this.pattern = null;
				e.printStackTrace();
			} catch (IllegalArgumentException e)
			{
				this.pattern = null;
				e.printStackTrace();
			} catch (Exception e)
			{
				this.pattern = null;
				e.printStackTrace();
			}
		}
	}



	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public Pattern getPattern() {
		return pattern;
	}
}
