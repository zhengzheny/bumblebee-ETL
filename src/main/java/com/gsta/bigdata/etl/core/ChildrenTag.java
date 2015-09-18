package com.gsta.bigdata.etl.core;

/**
 * 
 * @author tianxq
 * 
 */
public class ChildrenTag {
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
