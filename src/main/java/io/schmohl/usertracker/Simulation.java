package io.schmohl.usertracker;

import com.mongodb.WriteConcern;
import com.mongodb.async.client.MongoClient;
import com.mongodb.async.client.MongoClients;
import com.mongodb.async.client.MongoCollection;
import com.mongodb.async.client.MongoDatabase;
import com.mongodb.client.model.IndexOptions;
import org.bson.*;

import java.time.Instant;
import java.util.Arrays;

public class Simulation {
    public static void main(String[] args) throws Exception {
        int port = 8980;

        /**
         * recreate database on local host
         */
        MongoClient mongoClient = MongoClients.create(); // localhost for now....
        MongoDatabase database = mongoClient.getDatabase("userlocation");
        database.drop((x, t) -> {});

        /**
         * recreate userlocations collection w/ unique index on user_id and secondary index on venueid
         */

        MongoCollection<Document> userLocations = database
                .getCollection("userlocations")
                .withWriteConcern(WriteConcern.JOURNALED);

        userLocations.createIndex(
                new BsonDocument("_id", new BsonInt32(1)),
                new IndexOptions().unique(true),
                (x, t) -> {});

        userLocations.createIndex(new BsonDocument("venue_id", new BsonInt32(-1)), (x, t) -> {});

        /**
         * run the gRPC server
         */

        UserLocationServer server = new UserLocationServer(port, new Controller(userLocations));
        server.start();

        /**
         * add some users that visited McDonalds.
         * 2 of the users are still checked in (within 3 hours).
         */
        long now = Instant.now().getEpochSecond();
        Document checkedInUser = (new Document("_id", new BsonString("harry")))
                .append("venue_id", new BsonString("McDonalds"))
                .append("checkin_ts", new BsonInt64(now - 60*20)); // 20 minutes ago;
        Document checkedInUser2 = (new Document("_id", new BsonString("john")))
                .append("venue_id", new BsonString("McDonalds"))
                .append("checkin_ts", new BsonInt64(now - 60*60)); // 1 hour ago;
        Document checkedOutUser = (new Document("_id", new BsonString("sally")))
                .append("venue_id", new BsonString("McDonalds"))
                .append("checkin_ts", new BsonInt64(now - 60*60*4)); // 4 hours ago;

        userLocations.insertMany(
                Arrays.asList(checkedInUser, checkedInUser2, checkedOutUser),
                (result, t) -> System.out.println("inserted test users")
        );


        /**
         * run some client commands on local server
         */
        UserLocationClient client = new UserLocationClient("localhost", port);

        System.out.println("search for a user that's still checked in somewhere.");
        client.findUser("harry");
        System.out.println("search for a user whose last checkin was 4 hours ago.");
        client.findUser("sally");
        System.out.println("stream users currently checkedin at a location");
        client.findUsersAtVenue("McDonalds", 10);

        server.blockUntilShutdown();
    }
}
