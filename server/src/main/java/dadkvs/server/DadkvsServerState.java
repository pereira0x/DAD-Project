package dadkvs.server;

import dadkvs.DadkvsMain;
import dadkvs.DadkvsPaxos;
import dadkvs.DadkvsPaxosServiceGrpc;
import dadkvs.util.CollectorStreamObserver;
import dadkvs.util.GenericResponseCollector;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class DadkvsServerState {
    boolean        i_am_leader;
    int            debug_mode;
    int            base_port;
    int            my_id;
    int            store_size;
    KeyValueStore  store;
    MainLoop       main_loop;
    Thread         main_loop_worker;
	// for paxos
	// the lock
	private final Object paxosLock = new Object();
	private int proposalNumberCounter = 0;
	private int sequenceNumber = 0;
	private Map<Integer, ProposerState> proposerStates = new ConcurrentHashMap<>();
	private Map<Integer, AcceptorState> acceptorStates = new ConcurrentHashMap<>();
	// communication with other servers
	private final ManagedChannel[] serverChannels;
	private final DadkvsPaxosServiceGrpc.DadkvsPaxosServiceStub[] paxosStubs;
	private String[] targets;
	private String host;
	private int n_servers;

    public DadkvsServerState(int kv_size, int port, int myself) {
		base_port = port;
		my_id = myself;
		i_am_leader = my_id == 0;
		debug_mode = 0;
		store_size = kv_size;
		store = new KeyValueStore(kv_size);
		main_loop = new MainLoop(this);
		main_loop_worker = new Thread (main_loop);
		main_loop_worker.start();
		// communication with other servers
		this.n_servers = 5;
		this.host = "localhost";
		this.targets = new String[n_servers];
		this.serverChannels = new ManagedChannel[n_servers];
		this.paxosStubs = new DadkvsPaxosServiceGrpc.DadkvsPaxosServiceStub[n_servers];
		for (int i = 0; i < n_servers; i++) {
			int target_port = port + i;
			targets[i] = host + ":" + target_port;
			serverChannels[i] = ManagedChannelBuilder.forTarget(targets[i]).usePlaintext().build();
			paxosStubs[i] = DadkvsPaxosServiceGrpc.newStub(serverChannels[i]);
		}
		DadkvsServer.debug(this.getClass().getSimpleName(), "Am I the leader? " + i_am_leader);
    }

    public boolean isLeader() { return i_am_leader; }

	public boolean runPaxos(DadkvsMain.CommitRequest request) {
		DadkvsServer.debug(this.getClass().getSimpleName(), "Running paxos for request with reqid " + request.getReqid());
		synchronized(paxosLock) {
			int sequenceNumber = this.sequenceNumber++;
			int proposalNumber = generateProposalNumber();
			// creates the value to be proposed (the proposer state)
			ProposerState proposerState = new ProposerState(proposalNumber, request);
			proposerStates.put(sequenceNumber, proposerState);

			// phase 1
			boolean phaseOneResult = sendPrepareRequests(sequenceNumber, proposalNumber, proposerState);
			if (!phaseOneResult) {
				// TODO --> if phase one fails, what do we need to do?
				DadkvsServer.debug(this.getClass().getSimpleName(), "Phase one failed for sequence number " + sequenceNumber);
				return false;
			}
			// phase 2
			//boolean phaseTwoResult = sendAcceptRequests(sequenceNumber, proposalNumber, proposerState);
			//if (!phaseTwoResult) {
			//	// TODO --> if phase two fails, what do we need to do?
			//	DadkvsServer.debug(this.getClass().getSimpleName(), "Phase two failed for sequence number " + sequenceNumber);
			//	return false;
			//}
		}


		return true;
	}

	private boolean sendPrepareRequests(int sequenceNumber, int proposalNumber, ProposerState proposerState) {
		int answers = 0; // promises received
		int totalAcceptors = 5;
		int majority = totalAcceptors / 2 + 1;
		// constructs request
		DadkvsPaxos.PhaseOneRequest phaseOneRequest = DadkvsPaxos.PhaseOneRequest.newBuilder()
				.setPhase1Index(sequenceNumber)
				.setPhase1Timestamp(proposalNumber)
				.build();
		ArrayList<DadkvsPaxos.PhaseOneReply> phaseOneReplies = new ArrayList<>();
		GenericResponseCollector<DadkvsPaxos.PhaseOneReply> phaseOneCollector = new GenericResponseCollector<>(phaseOneReplies, n_servers);
		for (DadkvsPaxosServiceGrpc.DadkvsPaxosServiceStub paxosStub : paxosStubs) {
			DadkvsServer.debug(this.getClass().getSimpleName(), "Sending prepare request with sequence number " + sequenceNumber +
					" and proposal number " + proposalNumber + " to " + paxosStub);
			CollectorStreamObserver<DadkvsPaxos.PhaseOneReply> phaseOneObserver = new CollectorStreamObserver<>(phaseOneCollector);
			paxosStub.phaseone(phaseOneRequest, phaseOneObserver);
		}
		phaseOneCollector.waitForTarget(majority);
		// checks if we got a majority of promises
		int promises = 0;
		for (DadkvsPaxos.PhaseOneReply phaseOneReply: phaseOneReplies) {
			if (phaseOneReply.getPhase1Accepted()) {
				promises++;
			}
		}
		// for debug only
		if (promises >= majority) {
			DadkvsServer.debug(this.getClass().getSimpleName(), "Received majority of promises for sequence number " + sequenceNumber);
		} else {
			DadkvsServer.debug(this.getClass().getSimpleName(), "Did not receive majority of promises for sequence number " + sequenceNumber);
		}
		return promises >= majority;
	}

	private int generateProposalNumber() {
		// need to generate unique proposal numbers
		proposalNumberCounter++;
		return proposalNumberCounter * 10 + my_id;
	}

	public AcceptorState getOrCreateAcceptorState(int sequenceNumber) {
		AcceptorState acceptorState = acceptorStates.get(sequenceNumber);
		if (acceptorState == null) {
			acceptorState = new AcceptorState(0, null);
			acceptorStates.put(sequenceNumber, acceptorState);
		}
		return acceptorState;
	}
}
