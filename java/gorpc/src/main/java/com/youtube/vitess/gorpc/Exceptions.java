package com.youtube.vitess.gorpc;

public class Exceptions {
	@SuppressWarnings("serial")
	public static class GoRpcException extends Exception {
		public GoRpcException(String message) {
			super(message);
		}
	}

	@SuppressWarnings("serial")
	public static class ApplicationError extends Exception {
		public ApplicationError(String message) {
			super(message);
		}
	}

}
