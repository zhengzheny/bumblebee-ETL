package com.gsta.bigdata.etl.flume;

import com.gsta.bigdata.etl.ETLException;

import java.util.Locale;

public class FileNameHeaderFactory {
	@SuppressWarnings("unchecked")
	public Class<? extends IFileNameHeader> getClass(String type) throws ETLException {
		String typeClassName = type;
		FileNameHeaderType headerType = FileNameHeaderType.OTHER;
		try {
			headerType = FileNameHeaderType.valueOf(type.toUpperCase(Locale.ENGLISH));
		} catch (IllegalArgumentException ex) {
			throw new ETLException("HeaderType isn't a custom type", type);
		}
		if (!headerType.equals(FileNameHeaderType.OTHER)) {
			typeClassName = headerType.getTypeClassName();
		}
		try {
			return (Class<? extends IFileNameHeader>) Class.forName(typeClassName);
		} catch (Exception ex) {
			throw new ETLException("Unable to load source type: " + type
					+ ", class: " + typeClassName, ex);
		}
	}
}
