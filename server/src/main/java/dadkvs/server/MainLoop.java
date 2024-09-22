package dadkvs.server;

import java.util.ArrayList;
import java.util.Iterator;

/* these imported classes are generated by the hello-world-server contract */
import dadkvs.DadkvsMain;
import dadkvs.DadkvsPaxos;
import dadkvs.DadkvsPaxosServiceGrpc;

import dadkvs.util.GenericResponseCollector;
import dadkvs.util.CollectorStreamObserver;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;


public class MainLoop implements Runnable  {
    DadkvsServerState server_state;

    private boolean has_work;
    
    
    public MainLoop(DadkvsServerState state) {
	this.server_state = state;
	this.has_work = false;
    }

    public void run() {
	while (true) 
	    this.doWork();
    }
   
    
    
    synchronized public void doWork() {
	System.out.println("Main loop do work start");
	System.out.println("Am I the leader? " + this.server_state.isLeader());
	this.has_work = false;
	while (this.has_work == false) {
	    System.out.println("Main loop do work: waiting");
	    try {
		wait ();


		switch (this.server_state.debug_mode) {
			case 1:
				System.out.println("Server Crashed");
				System.exit(1);
				break;
		}
	    }
	    catch (InterruptedException e) {
	    }
	}
	System.out.println("Main loop do work finish");
    }
    
    synchronized public void wakeup() {
	this.has_work = true;
	notify();    
    }
}
