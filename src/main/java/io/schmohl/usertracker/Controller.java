package io.schmohl.usertracker;

import com.google.protobuf.Timestamp;
import com.mongodb.async.client.MongoCollection;
import static com.mongodb.client.model.Filters.*;

import com.mongodb.async.client.Observable;
import com.mongodb.async.client.Observables;
import com.mongodb.client.model.FindOneAndUpdateOptions;
import com.mongodb.client.model.ReturnDocument;
import org.bson.*;
import org.bson.conversions.Bson;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.CompletableFuture;

// most likely make this singleton and inject it with client pool and collection dependencies
// or make this implement Data Layer interface.
public class Controller {

    private final MongoCollection<Document> userLocations;
    // TODO: private final MongoCollection<Document> userLocationHistory;

    public Controller(MongoCollection<Document> userLocations) {
        this.userLocations = userLocations;
    }

    /**
     * fetches the user location if the record in the database is within 3 hours of client request.
     *
     * @param r FindUserRequest from protobuf.
     * @return future of FindUserResp
     */
    public CompletableFuture<FindUserResp> findUser(FindUserReq r) {

        final CompletableFuture<FindUserResp> f = new CompletableFuture<>();

        // search for a user by Id and within 3 hours of the request timestamp in case TTL is not exact.
        Bson query = and(
                eq("_id", r.getUserId()),
                gt("checkin_ts", r.getTs().getSeconds() - 60*60*3)
        );

        userLocations.find(query).into(new ArrayList<Document>(),
                (result, t) -> {
                    if (result == null) {
                        f.completeExceptionally(t);
                        return;
                    }

                    FindUserResp.Builder b = FindUserResp.newBuilder();

                    // if no records, complete future with match.
                    if( result.isEmpty() ) {
                        f.complete(b.setExists(false).build());
                        return;
                    }

                    // else we extract the user location (should be 1).
                    f.complete(FindUserResp.newBuilder()
                            .setExists(true)
                            .setLoc(userLocationFromDoc(result.get(0)))
                            .build()
                    );
                });

        return f;
    }


    /**
     * requires unique index on `_id`.
     *
     * idempotent, atomic update of a user's location follows these rules:
     *      Scenario 1: no document exists
     *          <- Action 1: insert the new record
     *      Scenario 2: document exists with a more recent checkin_ts, potentially written from another process/host
     *          <- Action 2: ignore op; query misses, and we handle a failed upsert due to unique index on `_id`.
     *      Scenario 3: document exists with a less recent checkin_ts, and we modify document with new `checkin_ts` and `venue_id`.
     *
     * @param r the `UserLocation` struct from client request.
     * @return a future of the CheckinResp, which tells us whether the venue or checkin_ts was updated.
     */
    public CompletableFuture<CheckinResp> addUserLoc(UserLocation r) {
        final CompletableFuture<CheckinResp> promise = new CompletableFuture<>();

        final long newCheckinTs = r.getCheckinTs().getSeconds();
        final String venueId = r.getVenueId();

        Bson query = and(
                eq("_id", r.getUserId()),
                lt("checkin_ts", newCheckinTs)
        );

        BsonDocument updates = new BsonDocument(Arrays.asList(
                new BsonElement("checkin_ts", new BsonInt64(newCheckinTs)),
                new BsonElement("venue_id", new BsonString(venueId))
        ));

        FindOneAndUpdateOptions opts = new FindOneAndUpdateOptions()
                .upsert(true)
                .returnDocument(ReturnDocument.BEFORE);

        userLocations.findOneAndUpdate(query, updates, opts, (result, t) -> {
            boolean isIgnore = (t != null && t.getMessage().contains("duplicate key err"));

            if (result == null && !isIgnore) {
                promise.completeExceptionally(t);
                return;
            }

            boolean isUpdated = !isIgnore && result.getLong("checkin_ts") == newCheckinTs && result.getString("venue_id").equals(venueId);

            promise.complete(CheckinResp.newBuilder()
                    .setUpdated(isUpdated)
                    .build()
            );
        });

        return promise;
    }

    /**
     * requires secondary index on {venue_id, ..[checkin_ts]}
     * server should implement observer.
     *
     * @param r request.
     * @return observable of users at venue based on request r.
     */
    public Observable<Document> findUsersAtVenue(FindVenueUsersReq r) {

        // TODO: add sorting by checkin_ts
        Bson query = and(
                eq("venue_id", r.getVenueId()),
                gt("checkin_ts", r.getTs().getSeconds() - 60*60*3)
        );

        return Observables.observe(userLocations.find(query).limit(r.getLimit()));
    }


    public static UserLocation userLocationFromDoc(Document checkin) {
        Timestamp checkinTs = Timestamp.newBuilder()
                .setSeconds(checkin.getLong("checkin_ts"))
                .build();

        return UserLocation.newBuilder()
                .setCheckinTs(checkinTs)
                .setUserId(checkin.getString("_id"))
                .setVenueId(checkin.getString("venue_id"))
                .build();
    }
}
