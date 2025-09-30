package com.github.yilativs.bb4jdbc;

/**
 * A functional interface for transforming an array of objects extracted from one row into array of objects that will be
 * used as a parameter for a batch.
 */
public interface Transformer {
	/**
	 * 
	 * @param results an array of objects extracted from one row
	 * @return array of objects that will be used as a parameter for a batch
	 */
	default Object[] transform(Object[] results) {
		return results;
	}
}
