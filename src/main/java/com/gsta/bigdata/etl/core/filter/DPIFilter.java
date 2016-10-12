package com.gsta.bigdata.etl.core.filter;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.HashSet;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Element;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.gsta.bigdata.etl.ETLException;
import com.gsta.bigdata.etl.core.Constants;
import com.gsta.bigdata.etl.core.ParseException;
import com.gsta.bigdata.etl.core.ShellContext;
import com.gsta.bigdata.utils.FileUtils;

public class DPIFilter extends AbstractFilter {
	private static final long serialVersionUID = 8913726868182314705L;
	@JsonProperty
	private final HashSet<String> suffixs = new HashSet<String>();
	private Logger logger = LoggerFactory.getLogger(getClass());

	public DPIFilter() {
		super();
	}

	@Override
	protected void initAttrs(Element element) throws ParseException {
		super.initAttrs(element);
		
		String urlSuffixFileName = super.getAttr("urlSuffixFile");
		logger.info("fileName=" + urlSuffixFileName);
		InputStream inputStream = null;
		try {
			inputStream = FileUtils.getInputFile(urlSuffixFileName);
			if (inputStream != null) {
				BufferedReader br = new BufferedReader(new InputStreamReader(inputStream));
				String line = null;
				while ((line = br.readLine()) != null) {
					if ("".equals(line) || line.startsWith("#")) {
						continue;
					}

					int pos = line.indexOf(".");
					// if line is ".tiff",put tiff to set;or put all line
					if (pos >= 0) {
						this.suffixs.add(line.substring(pos + 1));
					} else {
						this.suffixs.add(line);
					}
					logger.info(line);
				}
			}else{
				logger.info("input stream is null...");
			}
		}catch(Exception e) {
			throw new ParseException(e);
		}finally{
			if(inputStream != null){
				try {
					inputStream.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
	}

	@Override
	protected boolean accept(Map<String, String> filterData,
			ShellContext context) throws ETLException {
		if(filterData == null){
			return false;
		}
		
		String urlField = filterData.get(Constants.ATTR_URL);
		String host = filterData.get(Constants.ATTR_HOST);
		String str = "http://" + host + urlField;
		try {
			URL url = new URL(str);
			String path = url.getPath();
			int pos = path.lastIndexOf(".");
			if(pos > 0){
				String suffix = path.substring(pos + 1);
				return !this.suffixs.contains(suffix);
			}
		} catch (MalformedURLException e) {
			return false;
		}
		
		//if has no suffix,return true
		return true;
	}
}
