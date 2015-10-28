package com.gsta.bigdata.etl.core;

import java.io.Serializable;

/**
 * 
 * @author tianxq
 * 
 */
public class ChildrenTag implements Serializable{
	private static final long serialVersionUID = 3586681914772301268L;
	public final static int NODE = 1;
	public final static int NODE_LIST = 2;
	private String path;
	private int type;
	
	public ChildrenTag(String path, int type) {
		this.path = path;
		this.type = type;
	}

	public String getPath() {
		return path;
	}

	public int getType() {
		return type;
	}
	
	public String toString(){
		return "path=" + this.path + ",type=" + this.type;
	}
}
