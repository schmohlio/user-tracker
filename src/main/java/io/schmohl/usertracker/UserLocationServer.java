package io.schmohl.usertracker;

import io.grpc.Server;
import io.grpc.ServerBuilder;

import java.io.IOException;
import java.util.logging.Logger;

// TODO: start thread that reads from MessageBus and writes to MongoDB at throttled rate.
public class UserLocationServer {
    private static final Logger logger = Logger.getLogger(UserLocationServer.class.getName());

    private final int port;
    private final Server server;

    public UserLocationServer(int port, Controller c) throws IOException {
        this.port = port;
        server = ServerBuilder.forPort(port)
                .addService(new UserTrackerImpl(c))
                .build();
    }

    /** Start serving requests. */
    public void start() throws IOException {
        server.start();
        logger.info("Server started, listening on " + port);
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                System.err.println("*** shutting down gRPC server since JVM is shutting down");
                UserLocationServer.this.stop();
                System.err.println("*** server shut down");
            }
        });
    }

    /** Stop serving requests and shutdown resources. */
    public void stop() {
        if (server != null) {
            server.shutdown();
        }
    }

    /**
     * Await termination on the main thread since the grpc library uses daemon threads.
     */
    public void blockUntilShutdown() throws InterruptedException {
        if (server != null) {
            server.awaitTermination();
        }
    }

}
