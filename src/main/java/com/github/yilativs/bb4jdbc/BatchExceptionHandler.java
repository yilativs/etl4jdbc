package com.github.yilativs.bb4jdbc;

public interface BatchExceptionHandler {
	
	default boolean isRetryable(Exception exception) {
		return false;
	}

}
