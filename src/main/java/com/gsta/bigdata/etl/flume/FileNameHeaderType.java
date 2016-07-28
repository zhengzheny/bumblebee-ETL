package com.gsta.bigdata.etl.flume;

public enum FileNameHeaderType {
	OTHER(null),
	EXFODPI("com.gsta.bigdata.etl.flume.ExfoDPIHeader");

	private final String typeClassName;

	private FileNameHeaderType(String typeClassName) {
		this.typeClassName = typeClassName;
	}

	public String getTypeClassName() {
		return typeClassName;
	}
}
