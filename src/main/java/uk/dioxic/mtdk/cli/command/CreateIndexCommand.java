package uk.dioxic.mtdk.cli.command;

import com.mongodb.client.model.IndexOptions;
import org.bson.conversions.Bson;
import picocli.CommandLine;
import picocli.CommandLine.Option;
import uk.dioxic.mtdk.cli.mixin.MongoCommonMixin;

import java.util.concurrent.Callable;

@CommandLine.Command(name = "createIndex", description = "Creates an index")
public class CreateIndexCommand extends MongoCommonMixin implements Callable<Integer> {

    @Option(names = {"--background"})
    private boolean background;

    @Option(names = {"--keys"}, description = "index key definition (e.g. { field: -1 } )", required = true)
    private Bson keys;

    @Override
    public Integer call() {

        IndexOptions options = new IndexOptions().background(background);

        String name = getSyncCollection().createIndex(keys, options);

        System.out.println("Index created as " + name);

        return 0;
    }
}
