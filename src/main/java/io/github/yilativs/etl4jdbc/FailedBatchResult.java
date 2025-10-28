package io.github.yilativs.etl4jdbc;

import java.time.Instant;
import java.util.List;

/**
 * Represents a batch result for a failed batch operation.
 */
public class FailedBatchResult extends BatchResult {

	private final Exception exception;

	/**
	 * Constructs a FailedBatchResult.
	 * @param exception the exception that caused the failure
	 * @param parameters the batch parameters
	 * @param beginTime the start time
	 * @param endTime the end time
	 */
	public FailedBatchResult(Exception exception, List<Object[]> parameters, Instant beginTime, Instant endTime) {
		super(parameters, beginTime, endTime);
		this.exception = exception;
	}

	/**
	 * Gets the exception that caused the batch to fail.
	 * @return the exception
	 */
	public Exception getException() {
		return exception;
	}

}