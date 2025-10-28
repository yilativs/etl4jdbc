package io.github.yilativs.etl4jdbc;

import java.time.Instant;
import java.util.List;

/**
 * Represents a batch of operations that have failed during execution and can not be re-run.
 */
public class TerminalFailureBatchResult extends FailedBatchResult {

	/**
	 * Constructs a TerminalFailureBatchResult.
	 * @param exception the exception that caused the failure
	 * @param parameters the batch parameters
	 * @param beginTime the start time
	 * @param endTime the end time
	 */
	public TerminalFailureBatchResult(Exception exception, List<Object[]> parameters, Instant beginTime, Instant endTime) {
		super(exception, parameters, beginTime, endTime);
	}

}