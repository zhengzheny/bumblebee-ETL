package com.gsta.bigdata.etl.core.source;

import java.util.Set;

import com.google.api.client.repackaged.com.google.common.base.Preconditions;
import com.gsta.bigdata.etl.ETLException;
import com.gsta.bigdata.etl.core.ETLData;
/**
 * 
 * @author Shine
 *
 */
public class ZteENODEBXML extends AbstractSourceMetaData {
	public ZteENODEBXML() {
		super();
	}

	@Override
	public ETLData parseLine(String line, Set<String> invalidRecords)
			throws ETLException, ValidatorException {
		Preconditions.checkNotNull(line, "data line is null");
		
		return null;
	}
}
