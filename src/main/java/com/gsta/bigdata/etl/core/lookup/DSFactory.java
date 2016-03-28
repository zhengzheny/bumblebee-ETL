package com.gsta.bigdata.etl.core.lookup;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import com.gsta.bigdata.etl.core.Constants;

/**
 * 
 * @author Shine
 * 
 */
@SuppressWarnings("rawtypes")
public class DSFactory implements Serializable{
	private static final long serialVersionUID = 8727333587438191620L;
	private static Map<String, Class> datasources = new HashMap<String, Class>();

	static {
		datasources.put(Constants.LKP_MYSQL_TYPE_DS,MySQLDS.class);
		datasources.put(Constants.LKP_FLAT_TYPE_DS, FlatDS.class);
		datasources.put(Constants.LKP_HDFS_TYPE_DS, HdfsDS.class);
		datasources.put(Constants.LKP_DIM_TYPE_DS, DIMDS.class);
	}

	public static AbstractDataSource getDataSourceByType(String type) {
		AbstractDataSource datasource = null;

		try {
			datasource = (AbstractDataSource) Class.forName(datasources.get(type).getName()).newInstance();
		} catch (InstantiationException | IllegalAccessException
				| ClassNotFoundException e) {
			e.printStackTrace();
		}
		return datasource;
	}
}
