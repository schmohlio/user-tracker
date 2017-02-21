package io.schmohl.usertracker;

import com.google.protobuf.Empty;
import com.mongodb.async.client.Observer;
import com.mongodb.async.client.Subscription;
import io.grpc.stub.StreamObserver;
import org.bson.*;

import java.util.concurrent.CompletableFuture;

public class UserTrackerImpl extends UserTrackerGrpc.UserTrackerImplBase {

    private final Controller controller;

    public UserTrackerImpl(Controller c) { controller = c; }

    @Override
    public void checkIn(UserLocation loc, StreamObserver<CheckinResp> streamObserver) {
        handleOne(controller.addUserLoc(loc), streamObserver);
    }

    @Override
    public void checkInAsync(UserLocation loc, StreamObserver<Empty> streamObserver) {
        throw new RuntimeException("not implemented: use RabbitMQ or other durable Message Bus");
    }

    @Override
    public void findUser(FindUserReq userRequest, StreamObserver<FindUserResp> streamObserver) {
        handleOne(controller.findUser(userRequest), streamObserver);
    }

    private <T> void handleOne(CompletableFuture<T> promise, StreamObserver<T> streamObserver) {
        promise.whenComplete((resp, t) -> {
            if ( t != null )
                streamObserver.onError(t);
            else
                streamObserver.onNext(resp);

            streamObserver.onCompleted();
        });

    }

    @Override
    public void findVenueUsers(FindVenueUsersReq venueUsersReq, StreamObserver<UserLocation> streamObserver) {

        controller.findUsersAtVenue(venueUsersReq).subscribe(new Observer<Document>() {

            private long batchSize = 20; // how much should we buffer in memory on application server?
            private long k = 0; // counter for batch
            private long n = 0; // total seen.
            private Subscription subscription;

            @Override
            public void onSubscribe(Subscription subscription) {
                this.subscription = subscription;
                subscription.request(batchSize);
            }

            @Override
            public void onNext(Document document) {
                streamObserver.onNext(Controller.userLocationFromDoc(document));

                if (++k == batchSize) {
                    subscription.request(batchSize);
                    k = 0;
                }
            }

            @Override
            public void onError(Throwable e) {
                streamObserver.onError(e);
            }

            @Override
            public void onComplete() {
                streamObserver.onCompleted();
            }
        });
    }

}
