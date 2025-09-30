package com.github.yilativs.bb4jdbc;

import java.time.Instant;
import java.util.List;

/**
 * A class representing a successful batch execution result.
 */
public class SuccessfulBatchResult extends BatchResult {

	private final int[] updateCounts;

	public SuccessfulBatchResult(List<Object[]> parameters, int[] updateCounts, Instant beginTime, Instant endTime) {
		super(parameters, beginTime, endTime);
		this.updateCounts = updateCounts;
	}
	
	public int[] getUpdateCounts() {
		return updateCounts;
	}

}
