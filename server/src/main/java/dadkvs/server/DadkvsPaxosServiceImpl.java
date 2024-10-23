package dadkvs.server;

import dadkvs.DadkvsPaxos;
import dadkvs.DadkvsPaxosServiceGrpc;
import dadkvs.server.DadkvsServerState.PaxosState;
import io.grpc.Context;
import io.grpc.stub.StreamObserver;

public class DadkvsPaxosServiceImpl extends DadkvsPaxosServiceGrpc.DadkvsPaxosServiceImplBase {

    DadkvsServerState server_state;

    public DadkvsPaxosServiceImpl(DadkvsServerState state) {
        this.server_state = state;

    }

    @Override
    public void phaseone(DadkvsPaxos.PhaseOneRequest request,
            StreamObserver<DadkvsPaxos.PhaseOneReply> responseObserver) {

        this.server_state.getFreezeMode().waitUntilUnfreezed();
        this.server_state.getSlowMode().waitUntilUnslowed();
        // receives prepare and sends promise
        // for debug purposes
        DadkvsServer.debug(this.getClass().getSimpleName(),
                "Receive a PREPARE request with round number: " + request.getPhase1RoundNumber());

        Context ctx = Context.current().fork();

        int paxosCounter = request.getPhase1Index();
        int proposedRoundNumber = request.getPhase1RoundNumber();
        PaxosState paxosState = this.server_state.getOrCreatePaxosState(proposedRoundNumber, paxosCounter);
        // we need to get a possible reqId that was agreed upon from the paxosState at the given index (paxosCounter)

        //int newReqId = this.server_state.checkTotalOrderIndexAvailability(proposedIndex); // TODO is this correct?
        ctx.run(() -> {
            // if the read_ts that I have is smaller than the roundNumber being proposed, I PROMISE to it
            if (proposedRoundNumber > paxosState.getReadTs()) {
                // TODO if we accepted, we set our read_ts = proposedRoundNumber
                this.server_state.setReadTs(proposedRoundNumber);
                // if the proposal number i'm getting is bigger than mine, I promise to accept it
                DadkvsServer.debug(this.getClass().getSimpleName(), "Accepting proposal roundNumber: " + proposedRoundNumber);
                //this.server_state.setLatestAcceptedRoundNumber(proposedRoundNumber);
                DadkvsPaxos.PhaseOneReply reply = DadkvsPaxos.PhaseOneReply.newBuilder()
                        .setPhase1Accepted(true)
                        .setPhase1Reqid(paxosState.getCurrentReqId())
                        .setPhase1Timestamp(paxosState.getWriteTs())
                        .setPhase1Config(this.server_state.getConfig())
                        .build();
                DadkvsServer.debug(this.getClass().getSimpleName(),
                        "Sending PROMISE with reqid %d and write_ts %d", paxosState.getCurrentReqId(), paxosState.getWriteTs());
                responseObserver.onNext(reply);
                responseObserver.onCompleted();
            } else {
                // the proposal number is smaller than mine; I promised to accept a bigger one,
                // so I reject this one
                DadkvsServer.debug(this.getClass().getSimpleName(), "Rejecting proposal roundNumber: " + proposedRoundNumber);
                DadkvsPaxos.PhaseOneReply reply = DadkvsPaxos.PhaseOneReply.newBuilder().setPhase1Accepted(false).setPhase1Config(this.server_state.getConfig()).build();
                responseObserver.onNext(reply);
                responseObserver.onCompleted();
            }
        });

    }

    @Override
    public void phasetwo(DadkvsPaxos.PhaseTwoRequest request,
            StreamObserver<DadkvsPaxos.PhaseTwoReply> responseObserver) {

        this.server_state.getFreezeMode().waitUntilUnfreezed();
        this.server_state.getSlowMode().waitUntilUnslowed();

        // for debug purposes
        DadkvsServer.debug(this.getClass().getSimpleName(),
                "Receive an ACCEPT-REQUEST request with round number %d and reqid %d\n", request.getPhase2RoundNumber(),
                request.getPhase2Reqid());

        int proposedRoundNumber = request.getPhase2RoundNumber();
        Context ctx = Context.current().fork();
        ctx.run(() -> {
            if (proposedRoundNumber == this.server_state.getReadTs()) { // TODO check condition
                // if the proposal number is the same as the one I promised to accept, I accept
                // the value
                // TODO if we accepted, we set our write_ts = proposedRoundNumber

                DadkvsServer.debug(this.getClass().getSimpleName(), "Accepting value of reqId %d ,will send ACCEPTED.",
                        request.getPhase2Reqid());
                // we set the write_ts to the roundNumber
                this.server_state.setWriteTs(proposedRoundNumber);
                //this.server_state.setLatestAcceptedRoundNumber(proposedRoundNumber);
                this.server_state.learn(request.getPhase2RoundNumber(), request.getPhase2Reqid());

                DadkvsPaxos.PhaseTwoReply reply = DadkvsPaxos.PhaseTwoReply.newBuilder().setPhase2Accepted(true).setPhase2Config(this.server_state.getConfig()).build();
                responseObserver.onNext(reply);
                responseObserver.onCompleted();

            } else {
                DadkvsServer.debug(this.getClass().getSimpleName(), "Rejecting value of reqID %d, will send REJECTED.",
                        request.getPhase2Reqid());
                DadkvsPaxos.PhaseTwoReply reply = DadkvsPaxos.PhaseTwoReply.newBuilder().setPhase2Accepted(false).setPhase2Config(this.server_state.getConfig()).build();
                responseObserver.onNext(reply);
                responseObserver.onCompleted();
            }
        });

    }

    @Override
    public void learn(DadkvsPaxos.LearnRequest request, StreamObserver<DadkvsPaxos.LearnReply> responseObserver) {
        this.server_state.getFreezeMode().waitUntilUnfreezed();
        this.server_state.getSlowMode().waitUntilUnslowed();

        // for debug purposes
        DadkvsServer.debug(this.getClass().getSimpleName(),
                "Receive a LEARN request with round number %d and reqid %d\n", request.getLearnroundnumber(),
                request.getLearnreqid());

        Context ctx = Context.current().fork();
        ctx.run(() -> {
            DadkvsServer.debug(this.getClass().getSimpleName(), "Accepting value of reqId %d, will send LEARN-ACCEPTED.",
                    request.getLearnreqid());
					
            // Checks if we have a majority to commit the request
            int majority = this.server_state.getNumberOfAcceptors() / 2 + 1;
            int learnCounter = this.server_state.getLearnCounter(request.getLearnreqid(), request.getLearnroundnumber());
            DadkvsServer.debug(this.getClass().getSimpleName(), "LearnCounter: %d, Majority: %d", learnCounter, majority);
            if (learnCounter < majority) {
                DadkvsServer.debug(this.getClass().getSimpleName(), "Not enough LEARN requests to commit the request. LearnCounter: %d, Majority: %d",
                        learnCounter, majority);
            } else {
                DadkvsServer.debug(this.getClass().getSimpleName(), "Learn Majority reached. LearnCounter: %d, Majority: %d",
                        learnCounter, majority);
                DadkvsServer.debug(this.getClass().getSimpleName(), "Committing request with reqId %d.", request.getLearnreqid());
                this.server_state.commitRequest(request.getLearnreqid());
            }

            DadkvsPaxos.LearnReply reply = DadkvsPaxos.LearnReply.newBuilder().setLearnaccepted(true).setLearnconfig(this.server_state.getConfig()).build();
            DadkvsServer.debug(this.getClass().getSimpleName(), "Sending LEARN-REPLY with round number %d and reqid %d\n",
                    request.getLearnroundnumber(), request.getLearnreqid());

            responseObserver.onNext(reply);
            responseObserver.onCompleted();
        });
    }

}
