package com.gsta.bigdata.etl.core.source;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.List;
import java.util.Set;

import org.w3c.dom.Element;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.gsta.bigdata.etl.ETLException;
import com.gsta.bigdata.etl.core.Constants;
import com.gsta.bigdata.etl.core.ETLData;
import com.gsta.bigdata.etl.core.Field;
import com.gsta.bigdata.etl.core.ParseException;
import com.gsta.bigdata.utils.StringUtils;

public class SimpleDPI extends AbstractSourceMetaData {
	private static final long serialVersionUID = -7882984472494300744L;
	@JsonProperty
	private String delimiter = "\\|";
	
	@Override
	protected void initAttrs(Element element) throws ParseException {
		super.initAttrs(element);
		this.delimiter = super.getAttr(Constants.ATTR_DELIMITER);
	}
	
	@Override
	public ETLData parseLine(String line, Set<String> invalidRecords)
			throws ETLException, ValidatorException {
		if(line == null || "".equals(line)){
			return null;
		}
		
		String[] data = StringUtils.splitByWrapper(line, 
				this.delimiter,super.getWrapper());
		
		if(data == null){
			throw new ETLException(ETLException.NULL_DATA_BY_SPLIT,"parse line data occur null,delimiter=" +
		        this.delimiter + ",wrapper=" + super.getWrapper());
		}
		
		if (data.length != super.getFields().size()) {
			throw new ETLException(ETLException.DATA_NOT_EQUAL_DEFINITION,"data " + line + " £¬record count="
					+ data.length + ",but source definition field count="
					+ super.getFields().size());
		}

		ETLData etlData = new ETLData();
		for (int i = 0; i < data.length; i++) {
			Field field = super.getFields().get(i);
			etlData.addData(field.getId(), data[i]);
		}
		
		String protocol = "http://";
		String httpStr = etlData.getValue("url");
		if(!httpStr.trim().startsWith(protocol)){
			httpStr = protocol + httpStr;
		}
		
		try {
			URL url = new URL(httpStr);
			
			etlData.addData("host",url.getHost());
			etlData.addData("path",url.getPath());
			etlData.addData("query",url.getQuery());
			String[] fields = url.getQuery().split("&");
			for(String field:fields){
				String[] queryField = field.split("=");
				if(queryField.length == 2){
					etlData.addData(queryField[0], queryField[1]);
				}
			}
		} catch (MalformedURLException e) {
			e.printStackTrace();
			return null;
		}
		
		return etlData;
	}

	@Override
	public List<ETLData> parseLine(String line) throws ETLException,
			ValidatorException {
		return null;
	}
	
	public static void main(String[] args){
		File file = new File("D:\\github\\bumblebee-ETL\\src\\main\\resources\\test\\dpi\\youku0420.txt");
        BufferedReader reader = null;
        try {
            reader = new BufferedReader(new FileReader(file));
            String tempString = null;
            int count = 0;
            while ((tempString = reader.readLine()) != null) {
            	String[] fields = tempString.split("\\|");
            	if(fields.length == 2){
            		System.out.println("---------------------------------");
            		System.out.println("count=" + count);
            		String ua = fields[0];
            		System.out.println("ua="+ua);
            		String urlStr = fields[1];
            		
            		URL url = new URL("http://" + urlStr);
        			
        			System.out.println("host="+url.getHost());
        			System.out.println("port="+url.getPort());
        			System.out.println("path="+url.getPath());
        			System.out.println("query="+url.getQuery());
					if (url.getQuery() != null) {
						String[] queryFields = url.getQuery().split("&");
						for (String field : queryFields) {
							System.out.println(field);
						}
					}
            	}
            	count++;
            }
            reader.close();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (reader != null) {
                try {
                    reader.close();
                } catch (IOException e1) {
                }
            }
        }	
	}
}
