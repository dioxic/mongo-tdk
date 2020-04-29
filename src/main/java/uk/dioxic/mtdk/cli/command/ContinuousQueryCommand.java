package uk.dioxic.mtdk.cli.command;

import com.mongodb.ReadConcern;
import com.mongodb.WriteConcern;
import com.mongodb.client.*;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import org.bson.BsonTimestamp;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.json.JsonWriterSettings;
import picocli.CommandLine;
import picocli.CommandLine.Mixin;
import uk.dioxic.mtdk.cli.mixin.MongoCommonMixin;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.function.Consumer;
import java.util.stream.Collector;
import java.util.stream.Collectors;

import static com.mongodb.client.model.Aggregates.*;
import static com.mongodb.client.model.Filters.*;
import static java.util.Collections.singletonList;
import static picocli.CommandLine.Command;
import static picocli.CommandLine.Option;

@Command(name = "continuousQuery", description = "continuous queries something")
public class ContinuousQueryCommand implements Callable<Integer> {

    @Mixin
    private MongoCommonMixin mongoCommonMixin;

    @Option(names = {"-q", "--query"}, description = "the query to run", required = true)
    private Bson query;

    private JsonWriterSettings jws = JsonWriterSettings.builder().indent(true).build();

    @Override
    public Integer call() throws IOException {

        // get the current replica set status
        Document rsStatus = mongoCommonMixin.getSyncDatabase("admin").runCommand(new Document("replSetGetStatus", 1));

        // get the majority commit point from the replicaset status
        BsonTimestamp mcp = rsStatus
                .get("optimes", Document.class)
                .get("readConcernMajorityOpTime", Document.class)
                .get("ts", BsonTimestamp.class);

        // get the mongo collection
        MongoCollection<Document> mc = mongoCommonMixin.getSyncCollection()
                .withReadConcern(ReadConcern.MAJORITY)
                .withWriteConcern(WriteConcern.MAJORITY);

        // get the initial document(s)
        System.out.println("initial document(s)");
        List<Document> docList = new ArrayList<>();
        mc.find(query).into(docList);

        List<Object> docIdList = docList.stream()
                .peek(printDoc)
                .map(doc -> doc.get("_id"))
                .collect(Collectors.toList());

        System.out.print("Press a key to start watching change stream");
        System.in.read();

        // start watching for changes to the documents from the mcp
        System.out.println("document(s) changes");
        mc.watch(singletonList(match(or(in("documentKey._id", docIdList), convertQuery(query)))))
                .startAtOperationTime(mcp)
                .forEach(printCsDoc);

        return 0;
    }

    private Document convertQuery(Bson query) {
        return ((Document)query).entrySet().stream()
                .collect(Collector.of(
                        Document::new,
                        (doc , es) -> doc.put("fullDocument." + es.getKey(), es.getValue()),
                        (left, right) -> { left.putAll(right); return left; }
                ));
    }

    private Consumer<ChangeStreamDocument<Document>> printCsDoc = cs -> System.out.println(cs.getDocumentKey() + " " + cs.getFullDocument() + " " + cs.getUpdateDescription());
    private Consumer<Document> printDoc = doc -> System.out.println(doc.toJson(jws));

    private void something() {
        // get MongoDB connection
        MongoClient client = MongoClients.create();

        // get the mongo collection
        MongoCollection<Document> mongoCollection = client.getDatabase("test")
                .getCollection("trade")
                .withReadConcern(ReadConcern.MAJORITY);

        // start watching for changes to the document(s) from the operation time index
        ChangeStreamIterable<Document> changeStream = mongoCollection.watch(singletonList(match(eq("documentKey._id", 123))));

        // get the initial document(s) state
        FindIterable<Document> findResults = mongoCollection.find(eq("_id", 123));
    }
}