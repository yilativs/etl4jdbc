package com.github.yilativs.bb4jdbc;

import java.sql.SQLException;
import java.time.Instant;

public class UpdateResult {
	
	public enum Status {
		SUCCESS, FAILURE
	}
	
	private final Status status;
	private final int updatedCount;
	private final SQLException sqlException;
	private final Instant beginTime;
	private final Instant endTime;
	
	public UpdateResult(Status status, int updatedCount, SQLException sqlException, Instant beginTime, Instant endTime) {
		this.status = status;
		this.updatedCount = updatedCount;
		this.sqlException = sqlException;
		this.beginTime = beginTime;
		this.endTime = endTime;
	}
	

}
