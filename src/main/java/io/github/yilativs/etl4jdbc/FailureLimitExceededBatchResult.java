package io.github.yilativs.etl4jdbc;

import java.time.Instant;
import java.util.List;

/**
 * Represents a batch of operations that have failed during execution.
 * This class can be extended to include details about the failure,
 * such as error messages, failed records, or any other relevant information.
 */
public class FailureLimitExceededBatchResult extends TerminalFailureBatchResult {

	/**
	 * Constructs a FailureLimitExceededBatchResult.
	 * @param exception the exception that caused the failure
	 * @param parameters the batch parameters
	 * @param beginTime the start time
	 * @param endTime the end time
	 */
	public FailureLimitExceededBatchResult(Exception exception, List<Object[]> parameters, Instant beginTime, Instant endTime) {
		super(exception, parameters, beginTime, endTime);
	}

}