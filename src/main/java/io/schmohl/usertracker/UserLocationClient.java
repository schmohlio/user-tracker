package io.schmohl.usertracker;

import com.google.protobuf.Timestamp;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

import java.time.Instant;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

public class UserLocationClient {
    private static final Logger logger = Logger.getLogger(UserLocationClient.class.getName());

    private final ManagedChannel channel;
    private final UserTrackerGrpc.UserTrackerBlockingStub blockingStub;

    /** Construct client for accessing UserLocation server at {@code host:port}. */
    public UserLocationClient(String host, int port) {
        this(ManagedChannelBuilder.forAddress(host, port).usePlaintext(true));
    }

    /** Construct client for accessing RouteGuide server using the existing channel. */
    public UserLocationClient(ManagedChannelBuilder<?> channelBuilder) {
        channel = channelBuilder.build();
        blockingStub = UserTrackerGrpc.newBlockingStub(channel);
    }

    public void shutdown() throws InterruptedException {
        channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
    }

    public void findUser(String userId) {
        long now = Instant.now().getEpochSecond();
        FindUserReq req = FindUserReq.newBuilder()
                .setUserId(userId)
                .setTs(Timestamp.newBuilder().setSeconds(now))
                .build();

        FindUserResp resp = blockingStub.findUser(req);

        if (!resp.getExists()) {
            System.out.format("did not find user <%s>\n", userId);
            return;
        }
        System.out.format("found user <%s> with location <%s>\n", userId, stripBlocks(resp.getLoc().toString()));
    }

    public void findUsersAtVenue(String venueId, int limit) {
        long now = Instant.now().getEpochSecond();
        FindVenueUsersReq req = FindVenueUsersReq.newBuilder()
                .setVenueId(venueId)
                .setTs(Timestamp.newBuilder().setSeconds(now))
                .setLimit(limit)
                .build();

        int k = 0;
        Iterator<UserLocation> it = blockingStub.findVenueUsers(req);

        System.out.format("\t users at venue <%s>\n", venueId);
        while (it.hasNext())
            System.out.format("\t\t %d found user: %s\n",++k, stripBlocks(it.next().toString()));

        System.out.format("\t done streaming users at venue <%s>\n", venueId);
    }

    private static String stripBlocks(String s) { return s.replace('\t',' ').replace('\n',' ');}

}
