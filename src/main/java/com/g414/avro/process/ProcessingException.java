package com.g414.avro.process;

/**
 * Exception class for processing errors. Someday, this may become a richer
 * collection of exceptions - for now, it's pretty basic.
 */
public class ProcessingException extends Exception {
	private static final long serialVersionUID = 1L;

	public ProcessingException() {
		super();
	}

	public ProcessingException(String message, Throwable cause) {
		super(message, cause);
	}

	public ProcessingException(String message) {
		super(message);
	}

	public ProcessingException(Throwable cause) {
		super(cause);
	}
}