package com.github.yilativs.bb4jdbc;

import java.time.Instant;
import java.util.List;

public class BatchResult {

	private final Instant beginTime;
	private final Instant endTime;
	private final List<Object[]> parameters;
	
	public BatchResult(List<Object[]> parameters, Instant beginTime, Instant endTime) {
		this.beginTime = beginTime;
		this.endTime = endTime;
		this.parameters = parameters;
	}

	public Instant getBeginTime() {
		return beginTime;
	}

	public Instant getEndTime() {
		return endTime;
	}

	public List<Object[]> getParameters() {
		return parameters;
	}


}
