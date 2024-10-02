package dadkvs.server;

import dadkvs.DadkvsMain;

public class PromiseState {
	// need to store the last value written/read by the most recent proposer
	private int read_ts;
	private int value;
	private int key;

	private int write_ts;

	public PromiseState(int read_ts, int key, int value, int write_ts) {
		this.read_ts = read_ts;
		this.value = value;
		this.key = key;
		this.write_ts = write_ts;
	}

}
