package io.github.yilativs.etl4jdbc;

import static java.time.Instant.now;
import static java.util.List.copyOf;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import java.util.stream.Stream;

import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

/**
 * Bridge class to transfer data from a source database to a target database. It fetches data from the source using a
 * provided SQL query and upserts it into the target using another SQL query. Each new row fetched from the source is
 * send to the target, row data can be optionally transformed, by default parameters are provided in the order of the
 * select statement. Fetch size, batch size, concurrency level, retry logic, and failure handling can be fine tuned.
 * 
 */
public class ETL {

	private static final Logger logger = LoggerFactory.getLogger(ETL.class);

	private static final BatchResult COMPLETE_EXECUTION_SIGNAL = null;

	/**
	 * DataSource to read from. Must not be null.
	 */
	private final DataSource sourceDataSource;

	/**
	 * SQL query to fetch data from sourceDataSource. Must not be null and must return at least one column.
	 */
	private final String sourceSql;

	/**
	 * Transformer to apply to each row fetched from source before inserting into target. Must not be null.
	 */
	private Transformer transformer;

	/**
	 * Exception handler to apply to validate if batch can be re-executed in case of failure. Must not be null.
	 */
	private BatchExceptionHandler exceptionHandler;

	/**
	 * DataSource to write to. Must not be null.
	 */
	private final DataSource targetDataSource;

	/**
	 * SQL query to execute on targetDataSource. Must not be null and must have same number of parameters as columns
	 * returned transformer.
	 */
	private final String targetSql;

	/**
	 * Number of rows to fetch from source query. Must be positive (>0).
	 */
	private int fetchSize = 1000;

	/**
	 * Number of statements execute in a batch. Must be positive (>0).
	 */
	private int batchSize = 5000;

	/**
	 * Capacity of queue for batch processing. Must be positive (>0). Once the limit is reached, the fetching thread will
	 * stop reading.
	 */
	private int batchQueueCapacity = 100;

	/**
	 * Number of times to retry a batch in case of failure if in BatchExceptionHandler considers this exception can be
	 * retried. Must be non-negative (>=0).
	 */
	private int batchRetryLimit = 0;

	/**
	 * Wait time between retries in milliseconds. Must be non-negative (>=0).
	 */
	private int timeBetweenRetries = 0;

	/**
	 * Maximum number of failed batches allowed before stopping the process. Must be non-negative (>=0).
	 */
	private int failedBatchLimit = 0;

	/**
	 * Number of concurrent threads to use for processing batches. Must be positive (>0).
	 */
	private int concurrencyLevel = 1;

	/**
	 * Number of milliseconds to wait for termination of executor service. Must be non-negative (>=0).
	 */
	private int timeToWaitOnInterrupt = 0;

	private ETL(DataSource sourceDataSource, String sourceSql, DataSource targetDataSource, String targetSql) {
		this.sourceDataSource = sourceDataSource;
		this.sourceSql = sourceSql;
		this.targetDataSource = targetDataSource;
		this.targetSql = targetSql;
	}

	/**
	 * Starts the ETL process. Returns a stream of BatchResult objects representing results of each batch operation.
	 * 
	 * @param sourceParams parameters for the source sql.
	 * @return {@link Stream<BatchResult>} a stream of batch results.
	 */
	public Stream<BatchResult> run(Object... sourceParams) throws ETLReaderException {
		return run(Optional.empty(), Optional.empty(), sourceParams);
	}

	/**
	 * Starts the ETL process. Returns a stream of BatchResult objects representing results of each batch operation.
	 * 
	 * @param mdcKey       mdcKey to propagate to created threads, have in mind that in current thread mdcKey must be set
	 *                     before calling the method.
	 * @param mdcValue     mdcValue to propagate to created threads.
	 * @param sourceParams parameters for the source sql.
	 * @return {@link Stream<BatchResult>} a stream of batch results.
	 */
	public Stream<BatchResult> run(Optional<String> mdcKey, Optional<String> mdcValue, Object... sourceParams) throws ETLReaderException {
		logger.info("Starting processissing.");

		// runs the rejected task in the thread that attempted to submit it, so that no reading is done when all workers are
		// busy
		ThreadPoolExecutor.CallerRunsPolicy rejectionPolicy = new ThreadPoolExecutor.CallerRunsPolicy();
		// Create executor with bounded queue for back pressure
		ThreadPoolExecutor executor = new ThreadPoolExecutor(concurrencyLevel, concurrencyLevel, 0L, MILLISECONDS, new LinkedBlockingQueue<>(batchQueueCapacity), rejectionPolicy);
		ExecutorCompletionService<BatchResult> completionService = new ExecutorCompletionService<>(executor);

		AtomicInteger totalBatchFailureCount = new AtomicInteger(0);
		AtomicBoolean shouldStop = new AtomicBoolean(false);
		AtomicInteger batchesSubmitted = new AtomicInteger(0);
		AtomicInteger batchesCompleted = new AtomicInteger(0);
		AtomicReference<Exception> readerTheadException = new AtomicReference<>();

		// Start a separate thread to read from source and submit batches for processing
		Thread readerThread = new Thread(() -> {
			setMDC(mdcKey, mdcValue);
			Connection connection = null;
			PreparedStatement preparedStatement = null;
			ResultSet resultSet = null;

			try {
				connection = sourceDataSource.getConnection();
				preparedStatement = connection.prepareStatement(sourceSql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
				preparedStatement.setFetchSize(fetchSize);
				setParameters(preparedStatement, sourceParams);
				resultSet = preparedStatement.executeQuery();
				int columnCount = resultSet.getMetaData().getColumnCount();
				final List<Object[]> currentBatch = new ArrayList<>(batchSize);
				while (resultSet.next() && !shouldStop.get()) {
					currentBatch.add(transformer.transform(getRow(resultSet, columnCount)));
					// if batch is full or we reached the end, submit for processing
					if (currentBatch.size() >= batchSize || (resultSet.isLast() && currentBatch.size() > 0)) {
						// we need to make a copy before submit because otherwise inside lambda copy may be executed when batch is cleared or
						// contains new values
						List<Object[]> batchParameters = copyOf(currentBatch);
						completionService.submit(() -> processBatch(mdcKey, mdcValue, batchParameters, batchesSubmitted.getAndIncrement(), shouldStop));
						if (logger.isDebugEnabled()) {
							logger.debug("{} of batches are currenly being processed.", executor.getActiveCount());
						}
						currentBatch.clear();
					}
				}
				if (resultSet.isLast()) {
					logger.info("Reading finihed. Total batches submitted: {}", batchesSubmitted.get());
				} else {
					// happens when shouldStop is true due to error in batch processing
					logger.warn("Reading aborted. Total batches submitted: {}", batchesSubmitted.get());
				}
			} catch (SQLException | RuntimeException e) {
				logger.error(e.getMessage() + " while reading from source datasource", e);
				shouldStop.set(true);
				readerTheadException.set(e);
			} finally {
				closeQuietly(resultSet);
				closeQuietly(preparedStatement);
				closeQuietly(connection);
				completionService.submit(() -> COMPLETE_EXECUTION_SIGNAL);
				removeMDC(mdcKey);
			}
		});

		readerThread.start();

		return Stream.generate((Supplier<BatchResult>) () -> {
			if (shouldStop.get()) {
				return COMPLETE_EXECUTION_SIGNAL;
			}

			try {
				Future<BatchResult> future = completionService.take();
				BatchResult result = future.get();
				batchesCompleted.incrementAndGet();

				if (result == COMPLETE_EXECUTION_SIGNAL) {
					if (logger.isDebugEnabled()) {
						logger.debug("A single to complete execution processing received.");
					}
					shouldStop.set(true);
					Exception readerException = readerTheadException.get();
					if (readerException != null) {
						// not a batch but reader thread exception has happen.
						throw new ETLReaderException("An exception happen in ETLReader because of " + readerException.getMessage(), readerTheadException.get());
					}
					return COMPLETE_EXECUTION_SIGNAL;
				} else {
					if (result instanceof FailedBatchResult) {
						if (result instanceof TerminalFailureBatchResult) {
							shouldStop.set(true);
							logger.error("Batch processing stopped because of terminal condition has been reached.");
						} else {
							int currentFailures = totalBatchFailureCount.incrementAndGet();
							if (currentFailures > failedBatchLimit) {
								shouldStop.set(true);
								FailedBatchResult failedBatchResult = (FailedBatchResult) result;
								result = new FailureLimitExceededBatchResult(failedBatchResult.getException(), result.getParameters(), result.getBeginTime(), result.getEndTime());
								logger.error("Reached max failures limit. Stopping processing!");
							}
						}
					}
				}
				return result;
			} catch (InterruptedException e) {
				// interruptions can occur during stream processing or executor shutdown
				logger.warn("Stream processing was interrupted", e);
				Thread.currentThread().interrupt();
				shouldStop.set(true);
				throw new ETLReaderException("Unexpected exception happen during batch processing ", e.getCause());
			} catch (ExecutionException e) {
				// should never happen as we catch all exceptions in processBatch
				logger.error("Error processing batch", e);
				shouldStop.set(true);
				throw new ETLReaderException("Unexpected exception happen during batch processing ", e.getCause());
			} finally {
				if (shouldStop.get()) {
					shutdownExecutor(executor);
				}
			}
		}).takeWhile(batchResult -> batchResult != COMPLETE_EXECUTION_SIGNAL).onClose(() -> {
			// Cleanup when stream is closed
			if (logger.isDebugEnabled()) {
				logger.debug("Stream closed. Batches completed: {}/{}", batchesCompleted.get(), batchesSubmitted.get());
			}
			shouldStop.set(true);
		});
	}

	private Object[] getRow(ResultSet resultSet, int columnCount) throws SQLException {
		Object[] row = new Object[columnCount];
		for (int i = 0; i < columnCount; i++) {
			row[i] = resultSet.getObject(i + 1);
		}
		return row;
	}

	private void removeMDC(Optional<String> mdcKey) {
		if (mdcKey.isPresent()) {
			MDC.remove(mdcKey.get());
		}
	}

	private void setMDC(Optional<String> mdcKey, Optional<String> mdcValue) {
		if (mdcKey.isPresent() && mdcValue.isPresent()) {
			MDC.put(mdcKey.get(), mdcValue.get());
		}
	}

	private synchronized void shutdownExecutor(ExecutorService executorService) {
		if (executorService != null && !executorService.isShutdown()) {
			try {
				executorService.shutdown();
				if (!executorService.awaitTermination(timeToWaitOnInterrupt, TimeUnit.MILLISECONDS)) {
					if (!executorService.isShutdown()) {
						logger.warn("Executor service did not shutdown in provided {} milliseconds, forcing shutdown now", timeToWaitOnInterrupt);
						executorService.shutdownNow();
					}
				}
			} catch (InterruptedException e) {
				logger.warn("Executor service was interrupted during shutdown, will not shutdown in provided {} milliseconds, forcing shutdown now", timeToWaitOnInterrupt);
				executorService.shutdownNow();
				Thread.currentThread().interrupt();
			}
		}
	}

	private BatchResult processBatch(Optional<String> mdcKey, Optional<String> mdcValue, List<Object[]> batchParameters, int batchNumber, AtomicBoolean shouldStop) {
		try {
			setMDC(mdcKey, mdcValue);
			Instant beginTime = now();
			int attempt = 1;
			while (true) {
				Connection connection = null;
				PreparedStatement preparedStatement = null;
				try {
					if (shouldStop.get()) {
						return COMPLETE_EXECUTION_SIGNAL;
					}
					connection = targetDataSource.getConnection();
					connection.setAutoCommit(false);
					preparedStatement = connection.prepareStatement(targetSql);
					for (Object[] row : batchParameters) {
						setParameters(preparedStatement, row);
						preparedStatement.addBatch();
					}
					int[] results = preparedStatement.executeBatch();
					for (int i = 0; i < results.length; i++) {
						if (results[i] == PreparedStatement.EXECUTE_FAILED) {
							throw new SQLException("Batch statement at index " + i + " failed to execute");
						}
					}
					if (shouldStop.get()) {
						return COMPLETE_EXECUTION_SIGNAL;
					}
					connection.commit();
					if (logger.isDebugEnabled()) {
						logger.debug("Batch {} processed successfully on attempt {} in {}ms.", batchNumber, attempt, now().toEpochMilli() - beginTime.toEpochMilli());
					}
					return new SuccessfulBatchResult(batchParameters, results, beginTime, now());
				} catch (SQLException e) {
					logger.warn("Batch {} failed on attempt {} because of: {}", batchNumber, attempt, e.getMessage(), e);
					attempt++;
					if (connection != null) {
						try {
							connection.rollback();
						} catch (SQLException rollbackEx) {
							logger.warn("Failed to rollback transaction for batch {}", batchNumber, rollbackEx);
						}
					}
					if (attempt <= batchRetryLimit && exceptionHandler.isRetryable(e)) {
						try {
							Thread.sleep(timeBetweenRetries);
						} catch (InterruptedException ie) {

							shouldStop.set(true);
							logger.warn("Retry wait was interrupted for batch {}", batchNumber, ie);
							Thread.currentThread().interrupt();
							return new TerminalFailureBatchResult(e, batchParameters, beginTime, now());
						}
					} else {
						shouldStop.set(true);
						logger.error("Batch {} failed after {} attempts, because of: ", batchNumber, attempt, e.getMessage());
						return new FailedBatchResult(e, batchParameters, beginTime, now());
					}
				} finally {
					closeQuietly(preparedStatement);
					closeQuietly(connection);
				}
			}
		} finally {
			removeMDC(mdcKey);
		}
	}

	private void closeQuietly(AutoCloseable closeable) {
		if (closeable != null) {
			try {
				closeable.close();
			} catch (Exception e) {
				logger.warn("Failed to close resource", e);
			}
		}
	}

	private void setParameters(PreparedStatement ps, Object[] params) throws SQLException {
		for (int i = 0; i < params.length; i++) {
			// in jdbc parameters are 1-based
			ps.setObject(i + 1, params[i]);
		}
	}

	public static class Builder {
		// Mandatory fields
		private final DataSource sourceDataSource;
		private final String sourceSql;
		private final DataSource targetDataSource;
		private final String targetSql;

		// Optional fields - initialized to default values
		private Transformer transformer = (parameters) -> parameters;
		private BatchExceptionHandler exceptionHandler = exception -> false;
		private int fetchSize = 1000;
		private int batchSize = 5000;
		private int batchQueueCapacity = 100;
		private int batchRetryLimit = 0;
		private int timeBetweenRetries = 0;
		private int failedBatchLimit = 0;
		private int concurrencyLevel = 1;
		private int timeToWaitOnInterrupt = 0;

		public static Builder instance(DataSource sourceDataSource, String sourceSql, DataSource targetDataSource, String targetSql) {
			return new Builder(sourceDataSource, sourceSql, targetDataSource, targetSql);
		}

		private Builder(DataSource sourceDataSource, String sourceSql, DataSource targetDataSource, String targetSql) {
			if (sourceDataSource == null)
				throw new IllegalArgumentException("sourceDataSource must not be null");
			if (sourceSql == null)
				throw new IllegalArgumentException("sourceSql must not be null");
			if (targetDataSource == null)
				throw new IllegalArgumentException("targetDataSource must not be null");
			if (targetSql == null)
				throw new IllegalArgumentException("targetSql must not be null");
			this.sourceDataSource = sourceDataSource;
			this.sourceSql = sourceSql;
			this.targetDataSource = targetDataSource;
			this.targetSql = targetSql;
		}

		/**
		 * Sets the transformer to apply to each row. Must not be null.
		 * 
		 * @param transformer the transformer
		 * @return this builder
		 * @throws IllegalArgumentException if transformer is null
		 */
		public Builder transformer(Transformer transformer) {
			if (transformer == null)
				throw new IllegalArgumentException("transformer must not be null");
			this.transformer = transformer;
			return this;
		}

		/**
		 * Sets the exception handler. Must not be null.
		 * 
		 * @param exceptionHandler the exception handler
		 * @return this builder
		 * @throws IllegalArgumentException if exceptionHandler is null
		 */
		public Builder exceptionHandler(BatchExceptionHandler exceptionHandler) {
			if (exceptionHandler == null)
				throw new IllegalArgumentException("exceptionHandler must not be null");
			this.exceptionHandler = exceptionHandler;
			return this;
		}

		/**
		 * Sets the fetch size. Must be positive (>0).
		 * 
		 * @param fetchSize the fetch size
		 * @return this builder
		 * @throws IllegalArgumentException if fetchSize <= 0
		 */
		public Builder fetchSize(int fetchSize) {
			if (fetchSize <= 0)
				throw new IllegalArgumentException("fetchSize must be positive");
			this.fetchSize = fetchSize;
			return this;
		}

		/**
		 * Sets the batch size. Must be positive (>0).
		 * 
		 * @param batchSize the batch size
		 * @return this builder
		 * @throws IllegalArgumentException if batchSize <= 0
		 */
		public Builder batchSize(int batchSize) {
			if (batchSize <= 0)
				throw new IllegalArgumentException("batchSize must be positive");
			this.batchSize = batchSize;
			return this;
		}

		/**
		 * Sets the batches queue size. Must be positive (>0).
		 * 
		 * @param batchQueueCapacity the queue capacity
		 * @return this builder
		 * @throws IllegalArgumentException if batchQueueCapacity <= 0
		 */
		public Builder batchQueueCapacity(int batchQueueCapacity) {
			if (batchQueueCapacity <= 0)
				throw new IllegalArgumentException("batchQueueCapacity must be positive");
			this.batchQueueCapacity = batchQueueCapacity;
			return this;
		}

		/**
		 * Sets the batch retry limit. Must be non-negative (>=0).
		 * 
		 * @param batchRetryLimit the retry limit
		 * @return this builder
		 * @throws IllegalArgumentException if batchRetryLimit < 0
		 */
		public Builder batchRetryLimit(int batchRetryLimit) {
			if (batchRetryLimit < 0)
				throw new IllegalArgumentException("batchRetryLimit must be non-negative");
			this.batchRetryLimit = batchRetryLimit;
			return this;
		}

		/**
		 * Sets the time between retries in milliseconds. Must be non-negative (>=0).
		 * 
		 * @param timeBetweenRetries the time in ms
		 * @return this builder
		 * @throws IllegalArgumentException if timeBetweenRetries < 0
		 */
		public Builder timeBetweenRetries(int timeBetweenRetries) {
			if (timeBetweenRetries < 0)
				throw new IllegalArgumentException("timeBetweenRetries must be non-negative");
			this.timeBetweenRetries = timeBetweenRetries;
			return this;
		}

		/**
		 * Sets the failed batch limit. Must be non-negative (>=0).
		 * 
		 * @param failedBatchLimit the limit
		 * @return this builder
		 * @throws IllegalArgumentException if failedBatchLimit < 0
		 */
		public Builder failedBatchLimit(int failedBatchLimit) {
			if (failedBatchLimit < 0)
				throw new IllegalArgumentException("failedBatchLimit must be non-negative");
			this.failedBatchLimit = failedBatchLimit;
			return this;
		}

		/**
		 * Sets the concurrency level. Must be positive (>0).
		 * 
		 * @param concurrencyLevel the concurrency level
		 * @return this builder
		 * @throws IllegalArgumentException if concurrencyLevel <= 0
		 */
		public Builder concurrencyLevel(int concurrencyLevel) {
			if (concurrencyLevel <= 0)
				throw new IllegalArgumentException("concurrencyLevel must be positive");
			this.concurrencyLevel = concurrencyLevel;
			return this;
		}

		/**
		 * Sets the time to wait on interrupt in milliseconds. Must be non-negative (>=0).
		 * 
		 * @param timeToWaitOnInterrupt the time in ms
		 * @return this builder
		 * @throws IllegalArgumentException if timeToWaitOnInterrupt < 0
		 */
		public Builder timeToWaitOnInterrupt(int timeToWaitOnInterrupt) {
			if (timeToWaitOnInterrupt < 0)
				throw new IllegalArgumentException("timeToWaitOnInterrupt must be non-negative");
			this.timeToWaitOnInterrupt = timeToWaitOnInterrupt;
			return this;
		}

		public ETL build() {
			ETL etl = new ETL(sourceDataSource, sourceSql, targetDataSource, targetSql);
			etl.transformer = this.transformer;
			etl.exceptionHandler = this.exceptionHandler;
			etl.fetchSize = this.fetchSize;
			etl.batchSize = this.batchSize;
			etl.batchQueueCapacity = this.batchQueueCapacity;
			etl.batchRetryLimit = this.batchRetryLimit;
			etl.timeBetweenRetries = this.timeBetweenRetries;
			etl.failedBatchLimit = this.failedBatchLimit;
			etl.concurrencyLevel = this.concurrencyLevel;
			etl.timeToWaitOnInterrupt = this.timeToWaitOnInterrupt;
			return etl;
		}
	}

}