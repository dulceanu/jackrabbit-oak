package org.apache.jackrabbit.oak.run;

import org.apache.jackrabbit.oak.run.commons.Command;
import org.apache.jackrabbit.oak.segment.tool.JournalRecover;

import java.io.File;

import joptsimple.ArgumentAcceptingOptionSpec;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;

public class TarRecoverJournalCommand implements Command {

    @Override
    public void execute(String... args) throws Exception {
        OptionParser parser = new OptionParser();
        OptionSpec<String> directoryArg = parser.nonOptions(
                "Path to segment store (required)").ofType(String.class);
        ArgumentAcceptingOptionSpec<String> tarFile = parser.accepts(
                "tarFile", "name of the tar file to be checked for possible super roots")
                .withRequiredArg().ofType(String.class);
        OptionSet options = parser.parse(args);

        String path = directoryArg.value(options);

        if (path == null) {
            System.err.println("Recover an outdated journal from a file store. Usage: tar-recover-journal [path] --tarFile NAME_OF_TAR_TO_CHECK");
            parser.printHelpOn(System.err);
            System.exit(-1);
        }

        int code = JournalRecover.builder()
            .withPath(new File(path))
            .withTarFile(tarFile.value(options))
            .build()
            .run();

        System.exit(code);
    }
}
