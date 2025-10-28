package io.github.yilativs.etl4jdbc;

import java.time.Instant;
import java.util.List;

/**
 * Represents the result of a batch operation, including timing and parameters.
 */
public class BatchResult {

	private final Instant beginTime;
	private final Instant endTime;
	private final List<Object[]> parameters;
	
	/**
	 * Constructs a BatchResult.
	 * @param parameters the batch parameters
	 * @param beginTime the start time
	 * @param endTime the end time
	 */
	public BatchResult(List<Object[]> parameters, Instant beginTime, Instant endTime) {
		this.beginTime = beginTime;
		this.endTime = endTime;
		this.parameters = parameters;
	}

	/**
	 * Gets the time the batch started.
	 * @return the begin time
	 */
	public Instant getBeginTime() {
		return beginTime;
	}

	/**
	 * Gets the time the batch ended.
	 * @return the end time
	 */
	public Instant getEndTime() {
		return endTime;
	}

	/**
	 * Gets the parameters for the batch.
	 * @return the batch parameters
	 */
	public List<Object[]> getParameters() {
		return parameters;
	}


}