package dadkvs.server;

import dadkvs.DadkvsMain;

public class AcceptorState {
	// need to store the last vaiue written/read by the most recent proposer
	private int proposalNumber;
	private DadkvsMain.CommitRequest value;
	private int highestAcceptedProposalNumber = -1;
	private DadkvsMain.CommitRequest highestAcceptedValue = null;

	public AcceptorState(int proposalNumber, DadkvsMain.CommitRequest value) {
		this.proposalNumber = proposalNumber;
		this.value = value;
	}

}
