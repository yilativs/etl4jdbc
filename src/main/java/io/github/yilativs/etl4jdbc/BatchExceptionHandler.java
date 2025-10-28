package io.github.yilativs.etl4jdbc;

/**
 * A functional interface that defines batch exception handling.
 */
@FunctionalInterface
public interface BatchExceptionHandler {
	
	/** 
	 * Determines if the given exception is retryable.
	 *
	 * @param exception the exception to evaluate
	 * @return true if exception is retriable
	 */
	boolean isRetryable(Exception exception);

}