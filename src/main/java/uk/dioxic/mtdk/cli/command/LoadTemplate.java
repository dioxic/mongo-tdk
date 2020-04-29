package uk.dioxic.mtdk.cli.command;

import com.mongodb.MongoClientSettings;
import com.mongodb.client.model.InsertManyOptions;
import com.mongodb.reactivestreams.client.MongoCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine.Mixin;
import picocli.CommandLine.Parameters;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
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

@Command(name = "load",
        description = "loads random documents into MongoDB based on an mgenerate template")
public class LoadTemplate implements Callable<Integer> {

    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    @Mixin
    private MongoCommonMixin mongoCommonMixin;

    @Option(names = {"--drop"}, description = "drop collection before loading data (default: ${DEFAULT-VALUE})", defaultValue = "false")
    private boolean drop;

    @Option(names = {"-n", "--number"}, description = "number of documents to generate (default: ${DEFAULT-VALUE})", defaultValue = "5", paramLabel = "arg")
    private Integer number;

    @Option(names = {"-b", "--batchSize"}, description = "insert batch size (default: ${DEFAULT-VALUE})", defaultValue = "1000", paramLabel = "arg")
    private int batchSize;

    @Option(names = {"--concurrency"}, description = "reactive flapmap concurrency (default: ${DEFAULT-VALUE})", defaultValue = "3", paramLabel = "arg")
    private int concurrency;

    @Parameters(paramLabel = "TEMPLATE", description = "mgenerate template file or template string")
    private Template template;

    private final InsertManyOptions options;

    public LoadTemplate() {
        options = new InsertManyOptions();
        options.ordered(false);
        OperatorFactory.addBuilders("uk.dioxic.mtdk.operator");
    }

    @Override
    public Integer call() {

        mongoCommonMixin.applyToClientSettings(builder -> builder.codecRegistry(fromCodecs(new TemplateCodec())));

        MongoCollection<Template> mc = mongoCommonMixin.getAsyncCollection(Template.class);

        if (drop) {
            logger.info("dropping collection {}", mc.getNamespace().getCollectionName());
            Mono.from(mc.drop()).block();
        }

        Scheduler s = Schedulers.newElastic("executorPool");

        long start = System.currentTimeMillis();

//        Flux.range(0, number)
//                .publishOn(s)
//                .map(i -> template)
//                .map(Template::toRawBson)
//                .buffer(batchSize)
//                .flatMap(batch -> mc.insertMany(batch, options), concurrency)
//                .blockLast();

        Flux.range(0, number)
                .publishOn(s)
                .map(i -> template)
                .buffer(batchSize)
                .flatMap(batch -> mc.insertMany(batch, options), concurrency)
                .blockLast();

        double time = (double) (System.currentTimeMillis() - start) / 1000;
        double speed = number / time;

        System.out.println("populated " + number + " documents in " + Math.round(time) + "s (" + Math.round(speed) + " doc/s)");

        s.dispose();
        return 0;
    }
}