package com.github.yilativs.etl4jdbc;

import static java.lang.Long.valueOf;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mysql.MySQLContainer;
import org.testcontainers.postgresql.PostgreSQLContainer;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

@Testcontainers
public class ETLIT {
	@Container
	private static final PostgreSQLContainer postgres = new PostgreSQLContainer("postgres:18").withDatabaseName("source").withUsername("sourceUser").withPassword("sourcePassword");

	@Container
	private static final MySQLContainer mysql = new MySQLContainer("mysql:9.5").withDatabaseName("target").withUsername("targetUser").withPassword("targetPassword");

	@BeforeAll
	static void beforeAll() throws Exception {
		try (Connection pgConnection = DriverManager.getConnection(postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword()); Statement stmt = pgConnection.createStatement()) {
			stmt.executeUpdate("CREATE TABLE source (id INT PRIMARY KEY, name VARCHAR(255))");
			stmt.executeUpdate("INSERT INTO source VALUES (1, 'a'), (2, 'a'), (3, 'b')");
		}
		try (Connection mySqlConnection = DriverManager.getConnection(mysql.getJdbcUrl(), mysql.getUsername(), mysql.getPassword()); Statement stmt = mySqlConnection.createStatement()) {
			stmt.executeUpdate("CREATE TABLE target (name VARCHAR(255), total_count INT)");
		}
	}

	@BeforeEach
	void beforeEach() throws Exception {
		try (Connection mySqlConnection = DriverManager.getConnection(mysql.getJdbcUrl(), mysql.getUsername(), mysql.getPassword()); Statement stmt = mySqlConnection.createStatement()) {
			stmt.executeUpdate("DELETE FROM target");
		}
	}

	@Test
	void etlSelectsFromPostgresAndInsertsToMySqlIn3TheadsWithBatchSize1() throws Exception {

		// transfer data
		try (HikariDataSource pgDs = createDataSource(postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword(), postgres.getDriverClassName())) {
			try (HikariDataSource myDs = createDataSource(mysql.getJdbcUrl(), mysql.getUsername(), mysql.getPassword(), mysql.getDriverClassName())) {
				String sourceSql = "SELECT name, count(*) FROM source GROUP BY name";
				String targetSql = "INSERT INTO target values (?, ?)";
				AtomicInteger counter = new AtomicInteger();
				LinkedHashMap<Object, Object> expectedPairs = getExpectedPairs();
				int batchSize = 1;
				ETL.Builder.instance(pgDs, sourceSql, myDs, targetSql).batchSize(batchSize).batchesQueueSize(3).build().run().forEach(batchResult -> {
					counter.incrementAndGet();
					// we expect all results to be successful
					assertTrue(batchResult instanceof SuccessfulBatchResult);
					// because batch size is 1
					assertEquals(Integer.valueOf(batchSize), batchResult.getParameters().size());
					Object parameterNameValue = batchResult.getParameters().get(0)[0];
					Object parameterCountValue = batchResult.getParameters().get(0)[1];
					assertEquals(parameterCountValue, expectedPairs.get(parameterNameValue));
					// once pair found it is removed
					expectedPairs.remove(parameterNameValue);
				});

				assertEquals(2, counter.get());
				assertTrue(expectedPairs.isEmpty());
			}
		}
		// Verify results
		try (Connection conn = DriverManager.getConnection(mysql.getJdbcUrl(), mysql.getUsername(), mysql.getPassword());
		        PreparedStatement ps = conn.prepareStatement("SELECT name, total_count FROM target ORDER BY name asc");
		        ResultSet rs = ps.executeQuery()) {
			Map<Object, Object> expectedPairs = getExpectedPairs();
			while (rs.next()) {
				String nameValue = rs.getString("name");
				assertEquals(expectedPairs.get(nameValue), rs.getLong("total_count"));
				expectedPairs.remove(nameValue);
			}
			Assertions.assertTrue(expectedPairs.isEmpty());
		}
	}

	@Test
	void etlSelectsFromPostgresAndInsertsToMySqlIn1TheadsWithBatchSize10() throws Exception {

		// transfer data
		try (HikariDataSource pgDs = createDataSource(postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword(), postgres.getDriverClassName())) {
			try (HikariDataSource myDs = createDataSource(mysql.getJdbcUrl(), mysql.getUsername(), mysql.getPassword(), mysql.getDriverClassName())) {
				String sourceSql = "SELECT name, count(*) FROM source GROUP BY name";
				String targetSql = "INSERT INTO target values (?, ?)";
				AtomicInteger counter = new AtomicInteger();
				LinkedHashMap<Object, Object> expectedPairs = getExpectedPairs();
				int batchSize = 10;
				ETL.Builder.instance(pgDs, sourceSql, myDs, targetSql).batchSize(batchSize).batchesQueueSize(1).build().run().forEach(batchResult -> {
					counter.incrementAndGet();
					// we expect all results to be successful
					assertTrue(batchResult instanceof SuccessfulBatchResult);
					// batch size will be 2 and not 10 because we have only 2 results
					assertEquals(Integer.valueOf(2), batchResult.getParameters().size());
					Object parameterNameValueA = batchResult.getParameters().get(0)[0];
					Object parameterACountValue = batchResult.getParameters().get(0)[1];
					assertEquals(parameterACountValue, expectedPairs.get(parameterNameValueA));
					// once pair found it is removed
					expectedPairs.remove(parameterNameValueA);
					assertEquals(Integer.valueOf(2), batchResult.getParameters().size());
					Object parameterNameValueB = batchResult.getParameters().get(1)[0];
					Object parameterBCountValue = batchResult.getParameters().get(1)[1];
					assertEquals(parameterBCountValue, expectedPairs.get(parameterNameValueB));
					// once pair found it is removed
					expectedPairs.remove(parameterNameValueB);
				});

				assertEquals(1, counter.get());
				assertTrue(expectedPairs.isEmpty());
			}
		}
		// Verify results
		try (Connection conn = DriverManager.getConnection(mysql.getJdbcUrl(), mysql.getUsername(), mysql.getPassword());
		        PreparedStatement ps = conn.prepareStatement("SELECT name, total_count FROM target ORDER BY name asc");
		        ResultSet rs = ps.executeQuery()) {
			Map<Object, Object> expectedPairs = getExpectedPairs();
			while (rs.next()) {
				String nameValue = rs.getString("name");
				assertEquals(expectedPairs.get(nameValue), rs.getLong("total_count"));
				expectedPairs.remove(nameValue);
			}
			Assertions.assertTrue(expectedPairs.isEmpty());
		}
	}
	
	@Test
	void etlSelectsFromPostgresAndInsertsToMySqlIn1TheadsWithBatchSize10AndTransformer() throws Exception {

		// transfer data
		try (HikariDataSource pgDs = createDataSource(postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword(), postgres.getDriverClassName())) {
			try (HikariDataSource myDs = createDataSource(mysql.getJdbcUrl(), mysql.getUsername(), mysql.getPassword(), mysql.getDriverClassName())) {
				String sourceSql = "SELECT name, count(*) FROM source GROUP BY name";
				String targetSql = "INSERT INTO target values (?, ?)";
				AtomicInteger counter = new AtomicInteger();
				int batchSize = 10;
				ETL.Builder.instance(pgDs, sourceSql, myDs, targetSql).batchSize(batchSize).batchesQueueSize(1)
				.transformer(results -> new Object[] {((String)results[0]).toUpperCase(), ((Long)results[1]) * 2})
				.build().run().forEach(batchResult -> {
					counter.incrementAndGet();
					// we expect all results to be successful
					assertTrue(batchResult instanceof SuccessfulBatchResult);
					// batch size will be 2 and not 10 because we have only 2 results
					assertEquals(Integer.valueOf(2), batchResult.getParameters().size());
					assertEquals("A", batchResult.getParameters().get(0)[0]);
					assertEquals("B", batchResult.getParameters().get(1)[0]);
					assertEquals(4L, batchResult.getParameters().get(0)[1]);
					assertEquals(2L, batchResult.getParameters().get(1)[1]);
				});

				assertEquals(1, counter.get());
			}
		}
		// Verify results
		try (Connection conn = DriverManager.getConnection(mysql.getJdbcUrl(), mysql.getUsername(), mysql.getPassword());
		        PreparedStatement ps = conn.prepareStatement("SELECT name, total_count FROM target ORDER BY name asc");
		        ResultSet rs = ps.executeQuery()) {
			Map<Object, Object> expectedPairs = getExpectedPairs();
			while (rs.next()) {
				String nameValue = rs.getString("name").toLowerCase();
				assertEquals(expectedPairs.get(nameValue), rs.getLong("total_count") / 2);
				expectedPairs.remove(nameValue);
			}
			Assertions.assertTrue(expectedPairs.isEmpty());
		}
	}

	@Test
	void builderShouldThrowOnNullMandatoryParameters() {
		HikariDataSource ds = new HikariDataSource();
		Assertions.assertThrows(IllegalArgumentException.class, () -> ETL.Builder.instance(null, "sql", ds, "sql"));
		Assertions.assertThrows(IllegalArgumentException.class, () -> ETL.Builder.instance(ds, null, ds, "sql"));
		Assertions.assertThrows(IllegalArgumentException.class, () -> ETL.Builder.instance(ds, "sql", null, "sql"));
		Assertions.assertThrows(IllegalArgumentException.class, () -> ETL.Builder.instance(ds, "sql", ds, null));
	}

	@Test
	void builderShouldThrowOnNullTransformerOrExceptionHandler() {
		HikariDataSource ds = new HikariDataSource();
		ETL.Builder builder = ETL.Builder.instance(ds, "sql", ds, "sql");
		Assertions.assertThrows(IllegalArgumentException.class, () -> builder.transformer(null));
		Assertions.assertThrows(IllegalArgumentException.class, () -> builder.exceptionHandler(null));
	}

	@Test
	void builderShouldThrowOnNonPositiveFetchSizeBatchSizeBatchesQueueSizeConcurrencyLevel() {
		HikariDataSource ds = new HikariDataSource();
		ETL.Builder builder = ETL.Builder.instance(ds, "sql", ds, "sql");
		Assertions.assertThrows(IllegalArgumentException.class, () -> builder.fetchSize(0));
		Assertions.assertThrows(IllegalArgumentException.class, () -> builder.fetchSize(-1));
		Assertions.assertThrows(IllegalArgumentException.class, () -> builder.batchSize(0));
		Assertions.assertThrows(IllegalArgumentException.class, () -> builder.batchSize(-1));
		Assertions.assertThrows(IllegalArgumentException.class, () -> builder.batchesQueueSize(0));
		Assertions.assertThrows(IllegalArgumentException.class, () -> builder.batchesQueueSize(-1));
		Assertions.assertThrows(IllegalArgumentException.class, () -> builder.concurrencyLevel(0));
		Assertions.assertThrows(IllegalArgumentException.class, () -> builder.concurrencyLevel(-1));
	}

	@Test
	void builderShouldThrowOnNegativeBatchRetryLimitTimeBetweenRetriesFailedBatchLimitTimeToWaitOnInterrupt() {
		HikariDataSource ds = new HikariDataSource();
		ETL.Builder builder = ETL.Builder.instance(ds, "sql", ds, "sql");
		Assertions.assertThrows(IllegalArgumentException.class, () -> builder.batchRetryLimit(-1));
		Assertions.assertThrows(IllegalArgumentException.class, () -> builder.timeBetweenRetries(-1));
		Assertions.assertThrows(IllegalArgumentException.class, () -> builder.failedBatchLimit(-1));
		Assertions.assertThrows(IllegalArgumentException.class, () -> builder.timeToWaitOnInterrupt(-1));
	}

	private LinkedHashMap<Object, Object> getExpectedPairs() {
		LinkedHashMap<Object, Object> expectedPairs = new LinkedHashMap<>();
		expectedPairs.put("a", valueOf(2));
		expectedPairs.put("b", valueOf(1));
		return expectedPairs;
	}

	public static HikariDataSource createDataSource(String url, String user, String password, String driver) {
		HikariConfig config = new HikariConfig();
		config.setJdbcUrl(url);
		config.setUsername(user);
		config.setPassword(password);
		config.setDriverClassName(driver);
		config.setMaximumPoolSize(5);
		config.setConnectionTimeout(30000);
		config.setIdleTimeout(600000);
		config.setMaxLifetime(1800000);
		return new HikariDataSource(config);
	}
	
}