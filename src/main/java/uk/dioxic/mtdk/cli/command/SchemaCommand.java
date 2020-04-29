package uk.dioxic.mtdk.cli.command;

import com.mongodb.reactivestreams.client.MongoCollection;
import org.bson.BsonDocument;
import reactor.core.publisher.Flux;
import uk.dioxic.mtdk.cli.mixin.MongoCommonMixin;

import java.nio.file.Path;
import java.util.concurrent.Callable;

import static com.mongodb.client.model.Aggregates.sample;
import static java.util.Arrays.asList;
import static picocli.CommandLine.Command;
import static picocli.CommandLine.Option;

@Command(name = "schema", description = "analyses a schema to generate a template")
public class SchemaCommand extends MongoCommonMixin implements Callable<Integer> {

    @Option(names = {"-n", "--number"}, description = "number of documents to analyse (default: ${DEFAULT-VALUE})", defaultValue = "100")
    private Integer number;

    @Option(names = {"-o", "--out"}, description = "output file (default: CONSOLE)")
    private Path file;

    @Override
    public Integer call() {

        MongoCollection<BsonDocument> mc = getAsyncCollection(BsonDocument.class);

        Flux.from(mc.aggregate(asList(sample(number))))
                .map(doc -> doc.toJson())
                .last()
                .doOnNext(System.out::println)
                .block();

        return 0;
    }
}