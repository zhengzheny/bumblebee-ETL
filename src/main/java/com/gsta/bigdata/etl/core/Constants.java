package com.gsta.bigdata.etl.core;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import com.gsta.bigdata.etl.core.process.MRProcess;
import com.gsta.bigdata.etl.core.source.SimpleFlat;
import com.gsta.bigdata.etl.core.source.ZteENODEBXML;
import com.gsta.bigdata.etl.mapreduce.ETLMapper;

/**
 * 
 * @author tianxq
 *
 */
public class Constants {
	public final static String CONTEXT_PREFIX = "${";
	public final static String CONTEXT_POSTFIX = "}";
	public final static String CONTEXT_MONTH = "YYYYMM";
	
	//default value
	public final static String DEFAULT_COMPUTING_FRAMEWORK_MR = MRProcess.class.getSimpleName();
	public final static String DEFAULT_SOURCE_METADATA_FLAT = SimpleFlat.class.getSimpleName();
	public final static String DEFAULT_TRANSFORM_SCOPE = "map";
	public final static String DEFAULT_LKP_DS_PROPERTY_PATH = "path";
	public final static String DEFAULT_LKP_DS_TYPE = "flat";
	public final static String DEFAULT_ENCODING = "utf-8";
	public final static String DEFAULT_DELIMITER = "\001";
	
	//shell context
	public final static String SHELL_CTX_RESTART = "restart";
	
	//computing framework 
	public final static String CF_LOCAL_FILE_WRITE_COUNT = "recordWriteThreshold";
	public final static String CF_ERROR_RECORD_WRITE_COUNT = "errorRecordWriteThreshold";
	public final static String CF_LOCAL_FILE_BUFFER_SIZE = "localFileBufferSize";
	public final static String CF_LOCAL_FILE_DIR_MAX_FILE = "localFileDirMaxFile";
	public final static String CF_LOCAL_FILE_SLICE_THREAD_SIZE = "localFileSliceThreadSize";
	
	//local file's data source
	public final static String SOURCE_ZTE_NODEB_XML = ZteENODEBXML.class.getSimpleName();
	
	//lookup data source type
	public static final String LKP_FLAT_TYPE_DS = "flat";
	public static final String LKP_HDFS_TYPE_DS = "hdfs";
	public static final String LKP_MYSQL_TYPE_DS = "mysql";
	
	//element path in xml configure file
	public final static String PATH_PROCESS = "process";
	public final static String PATH_LOOKUP = "lookup";
	
	public final static String PATH_OUTPUT_METADATA = "outputMetaData";
	public final static String PATH_OUTPUT_METADATA_VALUES = "values";
	public final static String PATH_OUTPUT_METADATA_VALUES_FIELD = "values/field";
	public final static String PATH_MAP_OUTPUT_METADATA_KEYS = "keys";
	public final static String PATH_MAP_OUTPUT_METADATA_KEYS_FIELD = "keys/field";
	
	public final static String PATH_COMPUTING_FRAMEWORK_CONFIGS = "computingFrameworkConfigs";
	public final static String PATH_COMPUTING_FRAMEWORK_CONFIGS_PROPERTY ="property";
	
	public final static String PATH_SOURCE_METADATA = "sourceMetaData";
	public final static String PATH_SOURCE_METADATA_INPUT_PATHS = "paths/inputPath";
	public final static String PATH_SOURCE_METADATA_FIELDS = "fields/field";
	
	public final static String PATH_TRANSFORMS = "transforms";
	public final static String PATH_TRANSFORM = "transform";
	public final static String PATH_TRANSFORM_FILTER = "filter";
	public final static String PATH_TRANSFORM_FUNCTION = "function";
	
	public final static String PATH_LKP_TABLE = "table";
	public final static String PATH_LKP_TABLE_MAP = "map";
	
	public final static String PATH_LKP_DATASOURCES = "datasources";
	public final static String PATH_LKP_DATASOURCE = "datasource";
	public final static String PATH_LKP_DATASOURCE_PROPERTY = "property";
	public final static String PATH_LKP_DATASOURCE_FIELDS = "fields";
	public final static String PATH_LKP_DATASOURCE_FIELDS_FIELD = "fields/field";
	public final static String PATH_LKP_DATASOURCE_FIELDS_SQL = "fields/sql";
	
	//element attribute name
	public final static String ATTR_DELIMITER = "delimiter";
	public final static String ATTR_ID = "id";
	public final static String ATTR_KEY = "key";
	public final static String ATTR_VALUE = "value";
	public final static String ATTR_OUTPUT_PATH = "outputPath";
	public final static String ATTR_ERROR_PATH = "errorPath";
	public final static String ATTR_TYPE = "type";
	public final static String ATTR_PATH = "path";
	public final static String ATTR_INDEX = "index";
	public final static String ATTR_DESC = "desc";
	public final static String ATTR_SCOPE = "scope";
	public final static String ATTR_REF = "ref";
	public final static String ATTR_NAME = "name";
	public final static String ATTR_INPUT = "input";
	public final static String ATTR_OUTPUT = "output";
	public final static String ATTR_LIST = "list";
	public final static String ATTR_NOT_NULL = "notNull";
	public final static String ATTR_STRICT_CHECK = "strictCheck";
	public final static String ATTR_MIN_LENGTH = "minLength";
	public final static String ATTR_MAX_LENGTH = "maxLength";
	public final static String ATTR_BEGIN_POS = "beginPos";
	public final static String ATTR_END_POS = "endPos";
	public final static String ATTR_LENGTH = "length";
	public final static String ATTR_WRAPPER = "wrapper";
	public final static String ATTR_LOOKUP_TABLE = "lookupTable";
	public final static String ATTR_PROPERTY = "property";
	public final static String ATTR_FILE_SUFFIX = "fileSuffix";
	public final static String ATTR_FILENAME_PATTERN = "fileNamePattern";
	public final static String ATTR_CHARSET = "charset";
	
	public final static String TAG_FIELD = "field";
	public final static String TAG_INPUTPATH = "inputPath";
	public final static String TAG_WRITELOG = "writeLog";
	
	//mysql record fields
	public final static String LOG_RECORD_TABLE_NAME = "tableName";
	public final static String LOG_RECORD_STAT_DATE = "statDate";
	public final static String LOG_RECORD_TOTAL_NUMS = "totalNums";
	public final static String LOG_RECORD_SUCCESS_NUMS = "successNums";
	public final static String LOG_RECORD_FAIL_NUMS = "failNums";
	public final static String LOG_RECORD_END_TIME = "endTime";
	public final static String LOG_RECORD_COST_TIME = "costTime";
	public final static String LOG_RECORD_ERROR_PATH = "errorPath";

	//hadoop config
	public final static String HADOOP_CONF_MRPROCESS = "mrProcess";
	public final static String HADOOP_MAPPER_CLASS = "mapperClass";
	public final static String HADOOP_COMBINER_CLASS = "combinerClass";
	public final static String HADOOP_REDUCER_CLASS = "reducerClass";
	public final static String HADOOP_INPUTFORMAT_CLASS = "inputFormatClass";
	public final static String HADOOP_OUTPUTFORMAT_CLASS = "outputFormatClass";
	public final static String HADOOP_OUTPUTKEY_CLASS = "outputKeyClass";
	public final static String HADOOP_OUTPUTVALUE_CLASS = "outputValueClass";
	
	public final static String HADOOP_DEFAULT_MAPPER_CLASS = ETLMapper.class.getName();
	public final static String HADOOP_DEFAULT_INPUTFORMAT_CLASS = TextInputFormat.class.getName();
	public final static String HADOOP_DEFAULT_OUTPUTFORMAT_CLASS = TextOutputFormat.class.getName();
	public final static String HADOOP_IO_TEXT_CLASS = Text.class.getName(); 
}
