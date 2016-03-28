package com.gsta.bigdata.etl.core.lookup;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.gsta.bigdata.etl.core.Constants;
import com.gsta.bigdata.etl.core.LoadException;
import com.gsta.bigdata.etl.core.ParseException;
import com.gsta.bigdata.etl.core.source.mro.DIMObj;
import com.gsta.bigdata.utils.HdfsUtils;

public class DIMDS extends FlatDS {
	private static final long serialVersionUID = 1813144801854068504L;
	private Logger logger = LoggerFactory.getLogger(getClass());
	public final static String KEY_FIELD_DELIMITER = "-";

	public DIMDS() {
		super();
	}

	@Override
	public Map<String, Object> load(String key, String value)
			throws LoadException {
		if (null == key || "".equals(key)) {
			throw new LoadException("key is null.");
		}

		Map<String, String> properties = super.getProperties();
		List<String> paths = new ArrayList<String>();
		for (String strKey : properties.keySet()) {
			if (Constants.DEFAULT_LKP_DS_PROPERTY_PATH.equals(strKey)) {
				paths.add(properties.get(strKey));
			}
		}
		
		String[] keyFields = key.split(",",-1);

		// get hdfs result
		List<String> fields = super.getFields();
		Map<String, Object> retMap = new HashMap<String, Object>();
		for (String path : paths) {
			InputStream input = null;
			try {
				HdfsUtils hdfs = new HdfsUtils();
				input = hdfs.load(path);
				String line = null;
				BufferedReader reader = new BufferedReader(
						new InputStreamReader(input, Constants.DEFAULT_ENCODING));
				while ((line = reader.readLine()) != null) {
					if (!"".equals(line.trim())) {
						String[] lines = line.split(super.getDelimiter(), -1);
						if (lines.length != fields.size()) {
							throw new ParseException("dimension line:" + line
									+ ",datasource field count:"
									+ super.getFields().size()
									+ ",but line count:" + lines.length);
						}
						
						Map<String,String> data = new HashMap<String,String>();
						for (int i = 0; i < fields.size(); i++) {
							data.put(fields.get(i), lines[i]);
						}
						
						String keyStr = "";
						//get key string
						for(String str:keyFields){
							keyStr = keyStr + data.get(str) + KEY_FIELD_DELIMITER;
						}
						keyStr = keyStr.substring(0, keyStr.length() - KEY_FIELD_DELIMITER.length());
						
						DIMObj dimObj = new DIMObj(lines);
						retMap.put(keyStr, dimObj);
					}
				}

				IOUtils.closeStream(reader);
			} catch (Exception e) {
				e.printStackTrace();
				throw new LoadException(e);
			} finally {
				if (null != input) {
					try {
						input.close();
					} catch (IOException e) {
						e.printStackTrace();
						logger.error(e.toString());
					}
				}
			}
		}

		return retMap;
	}
}
