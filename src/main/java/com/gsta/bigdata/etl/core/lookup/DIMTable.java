package com.gsta.bigdata.etl.core.lookup;

public class DIMTable extends AbstractLKPTable {
	private static final long serialVersionUID = 8738059030705233503L;

	public DIMTable() {
		super();
	}

	@Override
	public Object getValue(String key, boolean isReverse) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void load() {
		if(super.dataSource == null){
			return;
		}
		
		String key = super.tableMaps.firstKey();
		super.dimensions = super.dataSource.load(key, null);
	}

}
