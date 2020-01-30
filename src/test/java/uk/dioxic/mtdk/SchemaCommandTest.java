package uk.dioxic.mtdk;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import picocli.CommandLine;
import uk.dioxic.mtdk.cli.command.SchemaCommand;

public class SchemaCommandTest {

    @Disabled
    @Test
    void execute_LocalDb_Success() {
        String[] args = "-d test -c test".split("\\s");
        new CommandLine(new SchemaCommand()).execute(args);
    }
}
