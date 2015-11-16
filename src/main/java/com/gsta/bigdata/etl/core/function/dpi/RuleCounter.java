package com.gsta.bigdata.etl.core.function.dpi;

import java.util.concurrent.atomic.AtomicLong;
/**
 * 
 * @author shine
 *
 */
public class RuleCounter {
	private static final RuleCounter instance = new RuleCounter();

	// GetURLClass counter
	private AtomicLong inputCounter = new AtomicLong();
	private AtomicLong matchingCounter = new AtomicLong();
	private AtomicLong matchedCounter = new AtomicLong();
	private AtomicLong invalidCounter = new AtomicLong();
	private AtomicLong unmatchedCounter = new AtomicLong();
	private AtomicLong level0Counter = new AtomicLong();
	private AtomicLong level1Counter = new AtomicLong();
	private AtomicLong level2Counter = new AtomicLong();
	private AtomicLong level3Counter = new AtomicLong();
	private AtomicLong level4Counter = new AtomicLong();
	private AtomicLong level5Counter = new AtomicLong();
	private AtomicLong level6Counter = new AtomicLong();
	private AtomicLong levelspamCounter = new AtomicLong();

	// ParseUserAgent counter
	private AtomicLong nTotalRecondsForRead = new AtomicLong();
	private AtomicLong nSkipRecordsForInvalidLen = new AtomicLong();
	private AtomicLong nSkipRecordsForNoKeywords = new AtomicLong();
	private AtomicLong nUpdatedRecords = new AtomicLong();
	private AtomicLong nUnmatchedRecords = new AtomicLong();
	private AtomicLong nCachedRecords = new AtomicLong();

	public RuleCounter() {

	}

	public static RuleCounter getInstance() {
		return instance;
	}

	public void setInputCounter() {
		this.inputCounter.getAndIncrement();
	}

	public void setMatchingCounter() {
		this.matchingCounter.getAndIncrement();
	}

	public void setMatchedCounter() {
		this.matchedCounter.getAndIncrement();
	}

	public void setInvalidCounter() {
		this.invalidCounter.getAndIncrement();
	}

	public void setUnmatchedCounter() {
		this.unmatchedCounter.getAndIncrement();
	}

	public void setLevel0Counter() {
		this.level0Counter.getAndIncrement();
	}

	public void setLevel1Counter() {
		this.level1Counter.getAndIncrement();
	}

	public void setLevel2Counter() {
		this.level2Counter.getAndIncrement();
	}

	public void setLevel3Counter() {
		this.level3Counter.getAndIncrement();
	}

	public void setLevel4Counter() {
		this.level4Counter.getAndIncrement();
	}

	public void setLevel5Counter() {
		this.level5Counter.getAndIncrement();
	}

	public void setLevel6Counter() {
		this.level6Counter.getAndIncrement();
	}

	public void setLevelspamCounter() {
		this.levelspamCounter.getAndIncrement();
	}

	public void setnTotalRecondsForRead() {
		this.nTotalRecondsForRead.getAndIncrement();
	}

	public void setnSkipRecordsForInvalidLen() {
		this.nSkipRecordsForInvalidLen.getAndIncrement();
	}

	public void setnSkipRecordsForNoKeywords() {
		this.nSkipRecordsForNoKeywords.getAndIncrement();
	}

	public void setnUpdatedRecords() {
		this.nUpdatedRecords.getAndIncrement();
	}

	public void setnUnmatchedRecords() {
		this.nUnmatchedRecords.getAndIncrement();
	}

	public void setnCachedRecords() {
		this.nCachedRecords.getAndIncrement();
	}

	public String getParseUserStatInfo() {
		String userAgentStatsMsg = "debug info------Useragent statistics in this mapper: \r\n"
				+ "\tTotal records:"
				+ nTotalRecondsForRead
				+ "\r\n"
				+ "\tinvalid length records: "
				+ nSkipRecordsForInvalidLen
				+ "\r\n"
				+ "\tno keyword records: "
				+ nSkipRecordsForNoKeywords
				+ "\r\n"
				+ "\tunmatched records: "
				+ nUnmatchedRecords
				+ "\r\n"
				+ "\tcached records: "
				+ nCachedRecords
				+ "\r\n"
				+ "\tlookuped records: " + nUpdatedRecords;
		return userAgentStatsMsg;
	}

	public String getUrlClassStatInfo() {
		String statInfo = "\r\n";
		matchedCounter.addAndGet(levelspamCounter.get() + level0Counter.get()
				+ level1Counter.get() + level2Counter.get()
				+ level3Counter.get() + level4Counter.get()
				+ level5Counter.get() + level6Counter.get());
		statInfo += "--total input records: " + inputCounter + "\r\n";
		statInfo += "--total invalid records: " + invalidCounter + "\r\n";
		statInfo += "--total unmatched records: " + unmatchedCounter + "\r\n";
		statInfo += "--total matched records: " + matchedCounter + "\r\n";
		statInfo += "----level-spam matched: " + levelspamCounter + "\r\n";
		statInfo += "----level-0 matched: " + level0Counter + "\r\n";
		statInfo += "----level-1 matched: " + level1Counter + "\r\n";
		statInfo += "----level-2 matched: " + level2Counter + "\r\n";
		statInfo += "----level-3 matched: " + level3Counter + "\r\n";
		statInfo += "----level-4 matched: " + level4Counter + "\r\n";
		statInfo += "--total matching times: " + matchingCounter + "\r\n";

		return statInfo;
	}

}
