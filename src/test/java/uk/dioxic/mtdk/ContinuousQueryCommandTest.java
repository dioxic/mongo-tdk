package uk.dioxic.mtdk;

import org.bson.Document;
import org.bson.conversions.Bson;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import picocli.CommandLine;
import uk.dioxic.mtdk.cli.command.ContinuousQueryCommand;

public class ContinuousQueryCommandTest {

    @Disabled
    @Test
    void execute_Success() {
        String[] args = "-d test -c a -q {_id:1}".split("\\s");
        new CommandLine(new ContinuousQueryCommand())
        .registerConverter(Bson.class, s -> Document.parse(s))
        .execute(args);
    }

}
