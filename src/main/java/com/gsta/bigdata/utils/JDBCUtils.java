package com.gsta.bigdata.utils;

import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import com.gsta.bigdata.etl.core.Constants;

/**
 * 
 * @author Shine
 * 
 */
public class JDBCUtils {
	private static final String[] JDBC_DRIVER_TYPE = { "mysql", "oracle","sqlserver" };

	private static final String JDBC_MYSQL_DRIVER = "com.mysql.jdbc.Driver";
	private static final String JDBC_ORACLE_DRIVER = "oracle.jdbc.driver.OracleDriver";
	private static final String JDBC_SQLSERVER_DRIVER = "com.microsoft.jdbc.sqlserver.SQLServerDriver";

	private static final String DRIVER = "driver";
	private static final String URL = "url";
	private static final String USERNAME = "username";
	private static final String PASSWORD = "password";

	private static Connection conn = null;
	private static PreparedStatement pstmt = null;
	private static ResultSet rs = null;

	private static Properties properties;

	private static Connection getConn(String path) throws Exception {
		try {
			InputStream in = FileUtils.getInputFile(path);
			properties = new Properties();
			properties.load(in);
			
			String driverName = properties.getProperty(DRIVER);
			String url = properties.getProperty(URL);
			String username = properties.getProperty(USERNAME);
			String password = properties.getProperty(PASSWORD);
			
			Class.forName(driverName);
			conn = DriverManager.getConnection(url, username, password);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return conn;
	}

	private static Connection getConn(String type, String url, String username,
			String password) throws Exception {
		String driverName = "";
		if (null == type || "".equals(type)) {
			throw new Exception("datasource type is unknow");
		}
		for (String strType : JDBC_DRIVER_TYPE) {
			if (strType.equals(type)) {
				if (type.equals(JDBC_DRIVER_TYPE[0])) {
					driverName = JDBC_MYSQL_DRIVER;
					break;
				} else if (type.equals(JDBC_DRIVER_TYPE[1])) {
					driverName = JDBC_ORACLE_DRIVER;
					break;
				} else if (type.equals(JDBC_DRIVER_TYPE[2])) {
					driverName = JDBC_SQLSERVER_DRIVER;
					break;
				}
			} else {
				throw new Exception("datasource type is unknow");
			}
		}
		try {
			Class.forName(driverName);
			conn = DriverManager.getConnection(url, username, password);
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
		return conn;
	}

	public static List<Map<String, Object>> getResult(String type, String url,
			String username, String password, String sql) throws Exception {
		conn = getConn(type, url, username, password);
		List<Map<String, Object>> resultList = new ArrayList<Map<String, Object>>();
		try {
			pstmt = conn.prepareStatement(sql);
			rs = pstmt.executeQuery();
			ResultSetMetaData metaData = rs.getMetaData();
			int columnCount = metaData.getColumnCount();
			while (rs.next()) {
				Map<String, Object> map = new HashMap<String, Object>();
				for (int i = 0; i < columnCount; i++) {
					String columnName = metaData.getColumnName(i + 1);
					Object columnValue = rs.getObject(columnName);
					map.put(columnName, columnValue);
				}
				resultList.add(map);
			}
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			closeAll(conn, pstmt, rs);
		}
		return resultList;
	}

	public static void recordLog(String path,Map<String, Object> params) throws Exception {
		conn = getConn(path);
		try {
			conn.setAutoCommit(false);
			pstmt = conn.prepareStatement("INSERT INTO t_etl_info values(?,?,?,?,?,?,?,?)");
			pstmt.setObject(1, params.get(Constants.LOG_RECORD_TABLE_NAME));
			pstmt.setObject(2, params.get(Constants.LOG_RECORD_STAT_DATE));
			pstmt.setObject(3, params.get(Constants.LOG_RECORD_TOTAL_NUMS));
			pstmt.setObject(4, params.get(Constants.LOG_RECORD_SUCCESS_NUMS));
			pstmt.setObject(5, params.get(Constants.LOG_RECORD_FAIL_NUMS));
			pstmt.setObject(6, params.get(Constants.LOG_RECORD_END_TIME));
			pstmt.setObject(7, params.get(Constants.LOG_RECORD_COST_TIME));
			pstmt.setObject(8, params.get(Constants.LOG_RECORD_ERROR_PATH));
			pstmt.executeUpdate();
			conn.commit();
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			closeAll(conn, pstmt, rs);
		}
	}

	private static void closeAll(Connection conn, PreparedStatement pstmt,
			ResultSet rs) {
		try {
			if (rs != null) {
				rs.close();
			}
			if (pstmt != null) {
				pstmt.close();
			}
			if (conn != null) {
				conn.close();
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
}
