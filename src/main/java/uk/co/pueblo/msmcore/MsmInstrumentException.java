package uk.co.pueblo.msmcore;

import uk.co.pueblo.msmcore.MsmInstrument.UpdateStatus;

public class MsmInstrumentException extends Exception {

	// Constants
	private static final long serialVersionUID = -7473261248696305187L;
	
	// Instance variables
	private final UpdateStatus updateStatus;

	MsmInstrumentException(String message, UpdateStatus updateStatus) {
		super(message);
		this.updateStatus = updateStatus;
	}

	public UpdateStatus getUpdateStatus() {
		return updateStatus;
	}
}