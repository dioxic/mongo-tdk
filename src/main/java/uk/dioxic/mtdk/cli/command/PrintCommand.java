package uk.dioxic.mtdk.cli.command;

import org.bson.json.JsonWriterSettings;
import picocli.CommandLine.Command;
import picocli.CommandLine.Parameters;
import uk.dioxic.mgenerate.core.Template;

import java.util.concurrent.Callable;

@Command(name = "print", description = "prints an mgenerate template to the console")
public class PrintCommand implements Callable<Integer> {

    @Parameters(paramLabel = "TEMPLATE", description = "mgenerate template file")
    Template template;

    @Override
    public Integer call() {
        System.out.println(template.toJson(JsonWriterSettings.builder().indent(true).build()));
        return 0;
    }
}
