package uk.dioxic.mtdk.cli.command;

import com.mongodb.client.*;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.Filters;
import org.bson.BsonTimestamp;
import org.bson.Document;
import org.bson.conversions.Bson;

import java.util.Arrays;

public class ChangeStream {

    public void start() {


        // get MongoDB connection
        MongoClient client = MongoClients.create();

        // start a new session
        ClientSession session = client.startSession();

        // get the mongo collection
        MongoCollection<Document> mongoCollection = client.getDatabase("test").getCollection("trade");

        // the match predicate for the document(s)
        Bson tradeQuery = Filters.eq("tradeId", 123);

        // get the initial trade document(s)
        FindIterable<Document> findResults = mongoCollection.find(session, tradeQuery);

        // get the operation time of the pervious find operation
        BsonTimestamp opTime = session.getOperationTime();

        // start watching for changes to the trades from the operation time index
        ChangeStreamIterable<Document> changeStream = mongoCollection.watch(session, Arrays.asList(Aggregates.match(tradeQuery))).startAtOperationTime(opTime);

    }

}
