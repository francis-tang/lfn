package one.inve.lfn.probe.common.cli;

import java.io.IOException;

/**
 * Exception class used by <code>ArgParser</code> when command line arguments
 * contain an error.
 * 
 * @see CliParser
 */
public class CliParseException extends IOException {
	/**
	 * Creates a new ArgParseException with the given message.
	 * 
	 * @param msg Exception message
	 */
	public CliParseException(String msg) {
		super(msg);
	}

	/**
	 * Creates a new ArgParseException from the given argument and message.
	 * 
	 * @param arg Offending argument
	 * @param msg Error message
	 */
	public CliParseException(String arg, String msg) {
		super(arg + ": " + msg);
	}
}
