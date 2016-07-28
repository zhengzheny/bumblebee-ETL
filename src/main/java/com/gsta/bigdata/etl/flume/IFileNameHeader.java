package com.gsta.bigdata.etl.flume;

import java.util.Map;

public interface IFileNameHeader {
	public Map<String, String> parseHeaders(String fileName);
}
