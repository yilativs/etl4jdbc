package io.github.yilativs.etl4jdbc;

import java.time.Instant;
import java.util.List;

/**
 * A class representing a successful batch execution result.
 */
public class SuccessfulBatchResult extends BatchResult {

	private final int[] updateCounts;

	/**
	 * Constructs a SuccessfulBatchResult.
	 * @param parameters the batch parameters
	 * @param updateCounts the update counts for each statement
	 * @param beginTime the start time
	 * @param endTime the end time
	 */
	public SuccessfulBatchResult(List<Object[]> parameters, int[] updateCounts, Instant beginTime, Instant endTime) {
		super(parameters, beginTime, endTime);
		this.updateCounts = updateCounts;
	}
	
	/**
	 * Gets the update counts for each statement in the batch.
	 * @return the update counts
	 */
	public int[] getUpdateCounts() {
		return updateCounts;
	}

}