package com.gsta.bigdata.etl.core.function.dpi;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * @author xiangy
 *
 */
public class MatchedUseragent {
	@JsonProperty
	private TerminalInfo terminalInfo;
	@JsonProperty
	private long updateTime;          

	public MatchedUseragent() {
	}

	public TerminalInfo getTerminalInfo() {
		return terminalInfo;
	}

	public void setTerminalInfo(TerminalInfo terminalInfo) {
		this.terminalInfo = terminalInfo;
	}

	public long getUpdateTime() {
		return updateTime;
	}

	public void setUpdateTime(long updateTime) {
		this.updateTime = updateTime;
	}
}
