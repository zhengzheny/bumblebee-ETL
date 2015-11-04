package com.gsta.bigdata.etl.core.function.dpi;

import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * @author xiangy
 *
 */
public class UseragentParserRule {
	@JsonProperty
	private String id = null;  		
	@JsonProperty
	private String regular = null;	
	@JsonProperty
	private Pattern pattern = null;	
	@JsonProperty
	private int[] indexes = null;	
	
	public UseragentParserRule() {
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}



	public String getRegular() {
		return regular;
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


	public Pattern getPattern() {
		return pattern;
	}


	public int[] getIndexes() {
		return indexes;
	}


	public void setIndexes(int[] indexes) {
		this.indexes = indexes;
	}
}
