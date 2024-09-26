package dadkvs.server;

import dadkvs.DadkvsMain;

public class ProposerState {
	private int proposalNumber;
	private DadkvsMain.CommitRequest value;
	private int highestAcceptedProposalNumber = -1;
	private DadkvsMain.CommitRequest highestAcceptedValue = null;

	public ProposerState(int proposalNumber, DadkvsMain.CommitRequest value) {
		this.proposalNumber = proposalNumber;
		this.value = value;
	}
}
