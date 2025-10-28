package io.github.yilativs.etl4jdbc;

/**
 * Exception thrown when an error occurs while reading data in ETL.
 */
public class ETLReaderException extends RuntimeException {

	private static final long serialVersionUID = 1L;

	/**
	 * Constructs an ETLReaderException with a message and cause.
	 * @param message the exception message
	 * @param cause the cause of the exception
	 */
	public ETLReaderException(String message, Throwable cause) {
		super(message, cause);
	}
	

}