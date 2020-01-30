package uk.dioxic.mtdk;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.model.Sorts;
import com.mongodb.connection.ClusterDescription;
import com.mongodb.connection.ConnectionPoolSettings;
import com.mongodb.connection.ServerDescription;
import com.mongodb.reactivestreams.client.MongoClient;
import com.mongodb.reactivestreams.client.MongoClients;
import com.mongodb.reactivestreams.client.MongoCollection;
import com.mongodb.selector.ServerSelector;
import org.bson.Document;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public class RunnerTest {
    Logger logger = LoggerFactory.getLogger(getClass());

    String uri = "mongodb://MarkBM-MacBook:27022,MarkBM-MacBook:27023,MarkBM-MacBook:27024,MarkBM-MacBook:27025,MarkBM-MacBook:27026,localhost:27026,localhost:28001,localhost:28002,localhost:27024,localhost:27025";

    @Disabled
    @Test
    void run() {
        System.out.println("test starting");

        Predicate<ServerDescription> primaryDC = desc -> desc.getAddress().getHost().contains("localhost");

        ServerSelector ss = clusterDescription -> {
            List<ServerDescription> allServers = clusterDescription.getServerDescriptions();

            List<ServerDescription> localServers = allServers.stream()
                    .filter(primaryDC)
                    .collect(Collectors.toList());

            return (localServers.size() > 2) ? localServers : allServers;
        };

        MongoClientSettings mcs = MongoClientSettings.builder()
                .applyToClusterSettings(builder -> builder.serverSelector(ss))
                .applyConnectionString(new ConnectionString(uri))
                .build();

        MongoClient client = MongoClients.create(mcs);

        Flux.from(client.startSession()).blockLast();

        MongoCollection<Document> collection = client.getDatabase("test").getCollection("a");

        Mono.from(collection.drop())
                .doOnSuccess(res -> System.out.println("drop completed"))
                .block();

        Flux.range(0, 10000)
                .map(i -> new Document("i", i))
                .buffer(1000)
                .flatMap(docs -> collection.insertMany(docs), 4)
                .doOnComplete(() -> System.out.println("insert completed"))
                .blockLast();

        Flux.from(collection.find().sort(Sorts.ascending("i")))
                .last()
                .doOnNext(System.out::println)
                .block();
    }

}
