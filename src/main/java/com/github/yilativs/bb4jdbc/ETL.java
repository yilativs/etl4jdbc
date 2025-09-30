package com.github.yilativs.bb4jdbc;

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
	 * DataSource to read from.
	 */
	private final DataSource sourceDataSource;

	/**
	 * SQL query to fetch data from sourceDataSource. Must return at least one column.
	 */
	private final String sourceSql;

	/**
	 * Transformer to apply to each row fetched from source before inserting into target.
	 */
	private Transformer transformer = new Transformer() {
	};

	/**
	 * Exception handler to apply to validate if batch can be re-executed in case of failure.
	 */
	private BatchExceptionHandler exceptionHandler = new BatchExceptionHandler() {
	};

	/**
	 * DataSource to write to.
	 */
	private final DataSource targetDataSource;

	/**
	 * SQL query to execute on targetDataSource. Must have same number of parameters as columns returned transformer.
	 */
	private final String targetSql;

	/**
	 * Number of rows to fetch from source query
	 */
	private int fetchSize = 1000;

	/**
	 * Number of statements execute in a batch.
	 */
	private int batchSize = 5000;

	/**
	 * Number of batches to keep in queue for processing. Once the limit is reached, the fetching thread will stop reading.
	 */
	private int batchesQueueSize = 100;

	/**
	 * Number of times to retry a batch in case of failure if in BatchExceptionHandler considers this exception can be
	 * retried.
	 */
	private int batchRetryLimit = 0;

	/**
	 * Wait time between retries in milliseconds.
	 */
	private int timeBetweenRetries = 0;

	/**
	 * Maximum number of failed batches allowed before stopping the process.
	 */
	private int failedBatchLimit = 0;

	/**
	 * Number of concurrent threads to use for processing batches.
	 */
	private int concurrencyLevel = 1;

	/**
	 * Number of milliseconds to wait for termination of executor service.
	 */
	private int timeToWaitOnInterrupt = 0;

	public ETL(DataSource sourceDataSource, String sourceSql, DataSource targetDataSource, String targetSql) {
		this.sourceDataSource = sourceDataSource;
		this.sourceSql = sourceSql;
		this.targetDataSource = targetDataSource;
		this.targetSql = targetSql;
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
	public Stream<BatchResult> update(Optional<String> mdcKey, Optional<String> mdcValue, Object... sourceParams) throws ETLReaderException {
		logger.info("Starting processissing.");

		// runs the rejected task in the thread that attempted to submit it, so that no reading is done when all workers are
		// busy
		ThreadPoolExecutor.CallerRunsPolicy rejectionPolicy = new ThreadPoolExecutor.CallerRunsPolicy();
		// Create executor with bounded queue for back pressure
		ThreadPoolExecutor executor = new ThreadPoolExecutor(concurrencyLevel, concurrencyLevel, 0L, MILLISECONDS, new LinkedBlockingQueue<>(batchesQueueSize), rejectionPolicy);
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
						completionService.submit(() -> processBatch(mdcKey, mdcValue, copyOf(currentBatch), batchesSubmitted.getAndIncrement(), shouldStop));
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
			logger.debug("Stream closed. Batches completed: {}/{}", batchesCompleted.get(), batchesSubmitted.get());
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
					logger.debug("Batch {} processed successfully on attempt {} in {}ms.", batchNumber, attempt, now().toEpochMilli() - beginTime.toEpochMilli());
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

}