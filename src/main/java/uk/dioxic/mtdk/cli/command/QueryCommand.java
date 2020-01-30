package uk.dioxic.mtdk.cli.command;

import com.mongodb.ConnectionString;
import com.mongodb.Function;
import com.mongodb.async.client.MongoClientSettings;
import com.mongodb.client.model.InsertManyOptions;
import com.mongodb.reactivestreams.client.MongoClient;
import com.mongodb.reactivestreams.client.MongoClients;
import com.mongodb.reactivestreams.client.MongoCollection;
import org.bson.Document;
import org.bson.RawBsonDocument;
import org.bson.conversions.Bson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import uk.dioxic.mgenerate.core.Template;
import uk.dioxic.mgenerate.core.codec.TemplateCodec;
import uk.dioxic.mgenerate.core.operator.OperatorFactory;

import java.util.concurrent.Callable;

import static org.bson.codecs.configuration.CodecRegistries.fromCodecs;
import static picocli.CommandLine.Command;
import static picocli.CommandLine.Option;

@Command(name = "query", description = "queries data using a query template")
public class QueryCommand implements Callable<Integer> {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Option(names = {"-h", "--help"}, usageHelp = true, description = "display a help message")
    private boolean helpRequested = false;

    @Option(names = {"-d", "--database"}, description = "mongo database", defaultValue = "test")
    private String database;

    @Option(names = {"-c", "--collection"}, description = "mongo collection", required = true)
    private String collection;

    @Option(names = {"-u", "--uri"}, description = "mongoUri", defaultValue = "mongodb://localhost:27017")
    private String mongoUri;

    @Option(names = {"-n", "--number"}, description = "number of documents to query (default: ${DEFAULT-VALUE})", defaultValue = "10000")
    private Integer number;

    @Option(names = {"-t", "--template"}, description = "template file", required = true)
    private Template template;

    @Option(names = {"--concurrency"}, description = "reactive flapmap concurrency (default: ${DEFAULT-VALUE})", defaultValue = "3")
    private int concurrency;

    private MongoClient client;
    private InsertManyOptions options;

    static RawBsonDocument d;

    QueryCommand() {
        options = new InsertManyOptions();
        options.ordered(false);
        //OperatorFactory.addBuilder(ImsiBuilder.class);
        OperatorFactory.addBuilders("org.mongodb.etl.operator");
    }

    @Override
    public Integer call() {

        MongoClientSettings mcs = MongoClientSettings.builder()
                .codecRegistry(fromCodecs(new TemplateCodec()))
                .applyConnectionString(new ConnectionString(mongoUri))
                .build();

        MongoClient client = MongoClients.create(mcs);
        MongoCollection<Document> mc = client
                .getDatabase(database)
                .getCollection(collection, Document.class);


        Scheduler s = Schedulers.newElastic("executorPool");

        Function<Bson, Long> timeQuery = (query) -> {
            long start = System.currentTimeMillis();
            mc.find(query);

            return System.currentTimeMillis() - start ;
        };

//        Flux.range(0, number)
//                .publishOn(s)
//                .map(i -> template.toRawBson())
//                .flatMap(q -> mc.find(q), concurrency)
//                .flatMap(batch -> mc.find(batch, options), concurrency)
//                .blockLast();

        long start = 0;
        double time = (double) (System.currentTimeMillis() - start) / 1000;
        double speed = number / time;

        System.out.println("populated " + number + " documents in " + Math.round(time) + "s (" + Math.round(speed) + " doc/s)");

        s.dispose();
        return 0;
    }

}