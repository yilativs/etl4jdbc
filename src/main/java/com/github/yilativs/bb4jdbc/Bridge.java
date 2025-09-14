package com.github.yilativs.bb4jdbc;

import java.util.stream.Stream;

import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Bridge class to transfer data from a source database to a target database.
 * It fetches data from the source using a provided SQL query and inserts it into the target using another SQL query.
 * Each new row fetched from the source is inserted into the target, parameters are provided in the order of the select statement.
 * The class supports configurable fetch size, batch size, concurrency level, retry logic, and failure handling.
 * 
 * Example usage:
 * <pre>
 * DataSource sourceDs = ...; // Initialize source DataSource
 * DataSource targetDs = ...; // Initialize target DataSource
 * String sourceQuery = "SELECT id, name FROM source_table WHERE status = ?";
 * String targetQuery = "INSERT INTO target_table (id, name) VALUES (?, ?)";
 * 
 * Bridge bridge = new Bridge(sourceDs, sourceQuery, targetDs, targetQuery);
 * bridge.setFetchSize(500);
 * bridge.setInsertBatchSize(200);
 * bridge.setConcurrencyLevel(4);
 * 
 * bridge.update("active").forEach(result -> {
 *     if (result.getStatus() == UpdateResult.Status.SUCCESS) {
 *         System.out.println("Inserted rows: " + result.getUpdatedCount());
 *     } else {
 *         System.err.println("Failed to insert rows: " + result.getSqlException().getMessage());
 *     }
 * });
 * </pre>
 */
public class Bridge {
	
	private static final Logger logger = LoggerFactory.getLogger(Bridge.class);
	
	private final DataSource sourceDataSource;
	private final String sourceSql;
	private final DataSource targetDataSource;
	private final String targetSql;

	/**
	 * Number of rows to fetch from source query
	 */
	private int fetchSize = 1000;
	
	/**
	 * Number of rows to insert in a single batch to target 
	 */
	private int insertBatchSize = 1000;
	
	/**
	 * Number of batches to keep in queue for processing, once the limit is reached, the fetching thread will wait.
	 */
	private int batchesQueueSize = 10;
	
	/**
	 * Number of times to retry in case of failure
	 */
	private int retryCount = 3;
	
	/**
	 * Maximum number of failures allowed before stopping the process.
	 */
	private int maxFailures = 10;
	
	/**
	 * Number of concurrent threads to use for processing batches.
	 */
	private int concurrencyLevel = 1;
	
	
	public Bridge(DataSource sourceDataSource, String sourceSql, DataSource targetDataSource, String targetSql) {
		this.sourceDataSource = sourceDataSource;
		this.sourceSql = sourceSql;
		this.targetDataSource = targetDataSource;
		this.targetSql = targetSql;
	}
	
	
	public Stream<UpdateResult> update(Object... sourceParams) {
	}

}
