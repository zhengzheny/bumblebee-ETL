package com.gsta.bigdata.etl.localFile;

import com.gsta.bigdata.etl.ETLException;
import com.gsta.bigdata.etl.core.Constants;
import com.gsta.bigdata.etl.core.process.LocalFileProcess;
import com.gsta.bigdata.utils.StringUtils;
/**
 * 
 * @author tianxq
 *
 */
public class HandlerFactory {
	public static AbstractHandler createHandler(LocalFileProcess process) throws ETLException {
		if (process == null) {
			throw new ETLException("process object is null.");
		}

		String type = process.getSourceType();
		type = StringUtils.upperCaseFirstChar(type);
		if (Constants.SOURCE_ZTE_NODEB_XML.equals(type)) {
			return new ZteNODEBXMLHandler(process);
		}else{
			//default is simple flat handler
			return new SimpleFlatHandler(process);
		}
	}
}
