package com.github.yilativs.etl4jdbc;

/**
 * A functional interface for transforming an array of objects extracted from one row into array of objects that will be
 * used as a parameter for a batch.
 */
@FunctionalInterface
public interface Transformer {
	/**
	 * 
	 * @param results an array of objects extracted from one row
	 * @return array of objects that will be used as a parameter for a batch
	 */
	 Object[] transform(Object[] results);
}
