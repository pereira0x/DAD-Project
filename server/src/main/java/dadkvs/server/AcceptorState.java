package dadkvs.server;

import dadkvs.DadkvsMain;

public class AcceptorState {
	// need to store the last value written/read by the most recent proposer
	private int read_ts;
	private DadkvsMain.CommitRequest value;
	private int write_ts;

	public AcceptorState(int read_ts, DadkvsMain.CommitRequest value, int write_ts) {
		this.read_ts = read_ts;
		this.value = value;
		this.write_ts = write_ts;
	}

}
