package com.github.yilativs.bb4jdbc;

import java.time.Instant;
import java.util.List;

/**
 * Represents a batch of operations that have failed during execution and can not be re-run.
 */
public class TerminalFailureBatchResult extends FailedBatchResult {

	public TerminalFailureBatchResult(Exception exception, List<Object[]> parameters, Instant beginTime, Instant endTime) {
		super(exception, parameters, beginTime, endTime);
	}

}
