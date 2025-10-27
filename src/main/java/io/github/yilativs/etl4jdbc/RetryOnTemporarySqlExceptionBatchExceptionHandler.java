package io.github.yilativs.etl4jdbc;

/**
 * A BatchExceptionHandler that retries on temporary SQL exceptions (SQLTransientException)
 */
public class RetryOnTemporarySqlExceptionBatchExceptionHandler implements BatchExceptionHandler{

	@Override
	public boolean isRetryable(Exception exception) {
		if (exception instanceof java.sql.SQLTransientException) {
			return true;
		}
		return false;
	}

}
