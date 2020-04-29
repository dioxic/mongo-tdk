package uk.dioxic.mtdk;

import org.bson.Document;
import org.bson.conversions.Bson;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Mixin;
import picocli.CommandLine.Option;
import uk.dioxic.mgenerate.core.Template;
import uk.dioxic.mgenerate.core.operator.OperatorFactory;
import uk.dioxic.mtdk.cli.command.*;
import uk.dioxic.mtdk.cli.mixin.FormattingMixin;
import uk.dioxic.mtdk.util.ContinuousQuery;

import java.nio.file.Paths;
import java.util.concurrent.Callable;

@Command(name = "mongo-tdk",
        header = "Mongo TDK v0.0.1",
        description = "Mongo Test Development Kit",
        subcommands = {
                LoadTemplate.class,
                PrintCommand.class,
                SchemaCommand.class,
                ContinuousQueryCommand.class,
                CreateIndexCommand.class
        }
)
public class Main implements Callable<Integer> {

    static {
        // add mgenerate operators
        OperatorFactory.addBuilders("uk.dioxic.mtdk.operator");
    }

    @Mixin
    private FormattingMixin formattingMixin;

    private static CommandLine cl;

    public static void main(String[] args) {
        cl = new CommandLine(new Main());
        cl.registerConverter(Template.class, s -> (s.startsWith("{")) ? Template.parse(s) : Template.from(s));
        cl.registerConverter(Bson.class, Document::parse);
        cl.setUsageHelpLongOptionsMaxWidth(40);
        System.exit(cl.execute(args));
    }

    @Override
    public Integer call() {
        cl.usage(System.out);
        return 0;
    }

}
