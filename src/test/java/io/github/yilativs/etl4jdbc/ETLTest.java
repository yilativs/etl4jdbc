package io.github.yilativs.etl4jdbc;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.stream.Stream;

import javax.sql.DataSource;

import org.junit.jupiter.api.Test;

public class ETLTest {

    @Test
    void testExceptionInSourceDataSourceThrowsETLReaderException() throws Exception {
        DataSource source = mock(DataSource.class);
        when(source.getConnection()).thenThrow(new SQLException("source error"));
        DataSource target = mock(DataSource.class);
        ETL etl = ETL.Builder.instance(source, "select * from foo", target, "insert into bar values (?)")
                .transformer(arr -> arr)
                .exceptionHandler(e -> false)
                .build();
        assertThrows(ETLReaderException.class, () -> etl.run().forEach(r -> {}));
    }

    @Test
    void testTransformerReturnsNullThrowsIllegalStateException() throws Exception {
        DataSource source = mock(DataSource.class);
        Connection conn = mock(Connection.class);
        PreparedStatement ps = mock(PreparedStatement.class);
        ResultSet rs = mock(ResultSet.class);
        when(source.getConnection()).thenReturn(conn);
        when(conn.prepareStatement(anyString(), anyInt(), anyInt())).thenReturn(ps);
        when(ps.executeQuery()).thenReturn(rs);
        when(rs.getMetaData()).thenReturn(mock(java.sql.ResultSetMetaData.class));
        when(rs.getMetaData().getColumnCount()).thenReturn(1);
        when(rs.next()).thenReturn(true, false);
        when(rs.getObject(1)).thenReturn("foo");
        DataSource target = mock(DataSource.class);
        ETL etl = ETL.Builder.instance(source, "select * from foo", target, "insert into bar values (?)")
                .transformer(arr -> null)
                .exceptionHandler(e -> false)
                .build();
        ETLReaderException ex = assertThrows(ETLReaderException.class, () -> etl.run().forEach(r -> {}));
        assertNotNull(ex.getCause());
        assertTrue(ex.getCause() instanceof IllegalStateException);
    }

    @Test
    void testExceptionInTargetDataSourceResultsInFailedBatchResult() throws Exception {
        DataSource source = mock(DataSource.class);
        Connection sourceConn = mock(Connection.class);
        PreparedStatement sourcePs = mock(PreparedStatement.class);
        ResultSet rs = mock(ResultSet.class);
        when(source.getConnection()).thenReturn(sourceConn);
        when(sourceConn.prepareStatement(anyString(), anyInt(), anyInt())).thenReturn(sourcePs);
        when(sourcePs.executeQuery()).thenReturn(rs);
        when(rs.getMetaData()).thenReturn(mock(java.sql.ResultSetMetaData.class));
        when(rs.getMetaData().getColumnCount()).thenReturn(1);
        when(rs.next()).thenReturn(true, false);
        when(rs.getObject(1)).thenReturn("foo");
        DataSource target = mock(DataSource.class);
        Connection targetConn = mock(Connection.class);
        PreparedStatement targetPs = mock(PreparedStatement.class);
        when(target.getConnection()).thenReturn(targetConn);
        when(targetConn.prepareStatement(anyString())).thenReturn(targetPs);
        doThrow(new SQLException("target error")).when(targetPs).executeBatch();
        doNothing().when(targetConn).rollback();
        doNothing().when(targetConn).setAutoCommit(false);
        ETL etl = ETL.Builder.instance(source, "select * from foo", target, "insert into bar values (?)")
                .transformer(arr -> arr)
                .exceptionHandler(e -> false)
                .batchSize(1)
                .build();
        Stream<BatchResult> stream = etl.run();
        BatchResult result = stream.findFirst().orElse(null);
        assertTrue(result instanceof FailedBatchResult);
        assertTrue(((FailedBatchResult) result).getException() instanceof SQLException);
    }

    @Test
    void testBuilderThrowsIllegalArgumentExceptionForNullsAndInvalids() {
        DataSource ds = mock(DataSource.class);
        assertThrows(IllegalArgumentException.class, () -> ETL.Builder.instance(null, "sql", ds, "sql"));
        assertThrows(IllegalArgumentException.class, () -> ETL.Builder.instance(ds, null, ds, "sql"));
        assertThrows(IllegalArgumentException.class, () -> ETL.Builder.instance(ds, "sql", null, "sql"));
        assertThrows(IllegalArgumentException.class, () -> ETL.Builder.instance(ds, "sql", ds, null));
        ETL.Builder builder = ETL.Builder.instance(ds, "sql", ds, "sql");
        assertThrows(IllegalArgumentException.class, () -> builder.transformer(null));
        assertThrows(IllegalArgumentException.class, () -> builder.exceptionHandler(null));
        assertThrows(IllegalArgumentException.class, () -> builder.fetchSize(0));
        assertThrows(IllegalArgumentException.class, () -> builder.batchSize(0));
        assertThrows(IllegalArgumentException.class, () -> builder.batchQueueCapacity(0));
        assertThrows(IllegalArgumentException.class, () -> builder.batchRetryLimit(-1));
        assertThrows(IllegalArgumentException.class, () -> builder.timeBetweenRetries(-1));
        assertThrows(IllegalArgumentException.class, () -> builder.failedBatchLimit(-1));
        assertThrows(IllegalArgumentException.class, () -> builder.concurrencyLevel(0));
        assertThrows(IllegalArgumentException.class, () -> builder.timeToWaitOnInterrupt(-1));
    }
}