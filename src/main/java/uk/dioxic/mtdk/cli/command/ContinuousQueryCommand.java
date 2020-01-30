package uk.dioxic.mtdk.cli.command;

import com.mongodb.ClientSessionOptions;
import com.mongodb.ReadConcern;
import com.mongodb.TransactionOptions;
import com.mongodb.WriteConcern;
import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.TransactionBody;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import org.bson.BsonTimestamp;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.json.JsonWriterSettings;

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
public class ContinuousQueryCommand extends MongoClientCommand implements Callable<Integer> {

    @Option(names = {"-q", "--query"}, description = "the query to run", required = true)
    private Bson query;

    private JsonWriterSettings jws = JsonWriterSettings.builder().indent(true).build();

    @Override
    public Integer call() throws IOException {

        // get the current replica set status
        Document rsStatus = getSyncDatabase("admin").runCommand(new Document("replSetGetStatus", 1));

        // get the majority commit point from the replicaset status
        BsonTimestamp mcp = rsStatus
                .get("optimes", Document.class)
                .get("readConcernMajorityOpTime", Document.class)
                .get("ts", BsonTimestamp.class);

        // get the mongo collection
        MongoCollection<Document> mc = getSyncCollection()
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
}