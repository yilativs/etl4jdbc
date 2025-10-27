package io.github.yilativs.etl4jdbc;

/**
 * A functional interface that defines batch exception handling.
 */
@FunctionalInterface
public interface BatchExceptionHandler {
	
	/** 
	 * 
	 * @param exception
	 * @return true if exception is retriable.
	 */
	boolean isRetryable(Exception exception);

}
