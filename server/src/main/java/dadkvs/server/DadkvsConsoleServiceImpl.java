package dadkvs.server;

/* these imported classes are generated by the contract */
import dadkvs.DadkvsConsole;
import dadkvs.DadkvsConsoleServiceGrpc;

import io.grpc.stub.StreamObserver;

public class DadkvsConsoleServiceImpl extends DadkvsConsoleServiceGrpc.DadkvsConsoleServiceImplBase {

	DadkvsServerState server_state;

	public DadkvsConsoleServiceImpl(DadkvsServerState state) {
		this.server_state = state;
	}

	@Override
	public void setleader(DadkvsConsole.SetLeaderRequest request,
			StreamObserver<DadkvsConsole.SetLeaderReply> responseObserver) {
		// for debug purposes
		System.out.println(request);

		boolean response_value = true;
		this.server_state.i_am_leader = request.getIsleader();

		// for debug purposes
		System.out.println("I am the leader = " + this.server_state.i_am_leader);

		this.server_state.main_loop.wakeup();

		DadkvsConsole.SetLeaderReply response = DadkvsConsole.SetLeaderReply.newBuilder()
				.setIsleaderack(response_value).build();

		responseObserver.onNext(response);
		responseObserver.onCompleted();
	}

	@Override
	public void setdebug(DadkvsConsole.SetDebugRequest request,
			StreamObserver<DadkvsConsole.SetDebugReply> responseObserver) {
		// for debug purposes
		System.out.println(request);

		boolean response_value = true;

		this.server_state.debug_mode = request.getMode();
		// for debug purposes
		System.out.println("Setting debug mode to = " + this.server_state.debug_mode);
		this.server_state.main_loop.wakeup();

		DadkvsConsole.SetDebugReply response = DadkvsConsole.SetDebugReply.newBuilder()
				.setAck(response_value).build();

		responseObserver.onNext(response);
		responseObserver.onCompleted();
	}
}
