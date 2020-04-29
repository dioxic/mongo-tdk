package uk.dioxic.mtdk.cli.command;

import com.mongodb.Function;
import com.mongodb.client.model.InsertManyOptions;
import com.mongodb.reactivestreams.client.MongoCollection;
import org.bson.Document;
import org.bson.RawBsonDocument;
import org.bson.conversions.Bson;
import picocli.CommandLine;
import picocli.CommandLine.Mixin;
import picocli.CommandLine.Parameters;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import uk.dioxic.mgenerate.core.Template;
import uk.dioxic.mgenerate.core.codec.TemplateCodec;
import uk.dioxic.mgenerate.core.operator.OperatorFactory;
import uk.dioxic.mtdk.cli.mixin.MongoCommonMixin;

import java.util.concurrent.Callable;

import static org.bson.codecs.configuration.CodecRegistries.fromCodecs;
import static picocli.CommandLine.Command;
import static picocli.CommandLine.Option;

@Command(name = "query", description = "queries data using a query template")
public class QueryCommand implements Callable<Integer> {

    @Mixin
    private MongoCommonMixin mongoCommonMixin;

    @Option(names = {"-n", "--number"}, description = "number of documents to generate (default: ${DEFAULT-VALUE})", defaultValue = "5")
    private Integer number;

    @Option(names = {"--concurrency"}, description = "reactive flapmap concurrency (default: ${DEFAULT-VALUE})", defaultValue = "3")
    private int concurrency;

    @Parameters(paramLabel = "TEMPLATE", description = "mgenerate template file")
    private Template template;

    @Override
    public Integer call() {

        mongoCommonMixin.applyToClientSettings(builder -> builder.codecRegistry(fromCodecs(new TemplateCodec())));

        MongoCollection<Document> mc = mongoCommonMixin.getAsyncCollection();

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