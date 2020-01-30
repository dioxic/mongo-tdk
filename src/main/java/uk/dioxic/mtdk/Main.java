package uk.dioxic.mtdk;

import org.bson.Document;
import org.bson.conversions.Bson;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import uk.dioxic.mgenerate.core.Template;
import uk.dioxic.mgenerate.core.operator.OperatorFactory;
import uk.dioxic.mtdk.cli.command.ContinuousQueryCommand;
import uk.dioxic.mtdk.cli.command.LoadTemplate;
import uk.dioxic.mtdk.cli.command.PrintCommand;
import uk.dioxic.mtdk.cli.command.SchemaCommand;

import java.nio.file.Paths;
import java.util.concurrent.Callable;

@Command(name = "mongo-tdk")
public class Main implements Callable<Integer> {

    static {
        // add mgenerate operators
        OperatorFactory.addBuilders("uk.dioxic.mtdk.operator");
    }

    @Option(names = {"-h", "--help"}, usageHelp = true, description = "display a help message")
    private boolean helpRequested = false;

    private static CommandLine cl;

    public static void main(String[] args) {
        cl = new CommandLine(new Main());
        cl.addSubcommand("load", new LoadTemplate());
        cl.addSubcommand("print", new PrintCommand());
        cl.addSubcommand("schema", new SchemaCommand());
        cl.addSubcommand("csQuery", new ContinuousQueryCommand());
        cl.registerConverter(Template.class, s -> (s.startsWith("{")) ? Template.parse(s) : Template.from(s));
        cl.registerConverter(Bson.class, s -> Document.parse(s));
        System.exit(cl.execute(args));
    }

    @Override
    public Integer call() {
        cl.usage(System.out);
        return 0;
    }

}
