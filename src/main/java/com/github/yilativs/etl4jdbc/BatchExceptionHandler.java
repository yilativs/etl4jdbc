package com.github.yilativs.etl4jdbc;

public interface BatchExceptionHandler {
	
	default boolean isRetryable(Exception exception) {
		return false;
	}

}
