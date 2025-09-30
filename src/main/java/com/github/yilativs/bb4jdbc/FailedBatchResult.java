package com.github.yilativs.bb4jdbc;

import java.time.Instant;
import java.util.List;

public class FailedBatchResult extends BatchResult {

	private final Exception exception;

	public FailedBatchResult(Exception exception, List<Object[]> parameters, Instant beginTime, Instant endTime) {
		super(parameters, beginTime, endTime);
		this.exception = exception;
	}

	public Exception getException() {
		return exception;
	}

}
