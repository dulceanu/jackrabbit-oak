package org.apache.jackrabbit.oak.segment.tool;

import static com.google.common.base.Preconditions.checkNotNull;

import org.apache.jackrabbit.oak.segment.RecordId;
import org.apache.jackrabbit.oak.segment.RecordType;
import org.apache.jackrabbit.oak.segment.Segment.RecordConsumer;
import org.apache.jackrabbit.oak.segment.SegmentId;
import org.apache.jackrabbit.oak.segment.SegmentNodeState;
import org.apache.jackrabbit.oak.segment.SegmentNotFoundException;
import org.apache.jackrabbit.oak.segment.file.FileStoreBuilder;
import org.apache.jackrabbit.oak.segment.file.InvalidFileStoreVersionException;
import org.apache.jackrabbit.oak.segment.file.ReadOnlyFileStore;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;

public class JournalRecover {

    public static Builder builder() {
        return new Builder();
    }

    /**
     * Collect options for the {@link Compact} command.
     */
    public static class Builder {

        private File path;

        private Builder() {
            // Prevent external instantiation.
        }

        /**
         * The path to an existing segment store. This parameter is required.
         *
         * @param path
         *            the path to an existing segment store.
         * @return this builder.
         */
        public Builder withPath(File path) {
            this.path = checkNotNull(path);
            return this;
        }

        /**
         * Create an executable version of the {@link JournalRecover} command.
         *
         * @return an instance of {@link Runnable}.
         */
        public JournalRecover build() {
            checkNotNull(path);
            return new JournalRecover(this);
        }
    }

    private final File path;

    private JournalRecover(Builder builder) {
        this.path = builder.path;
    }

    public int run() throws IOException, InvalidFileStoreVersionException {
        try (final ReadOnlyFileStore fileStore = FileStoreBuilder.fileStoreBuilder(path).buildReadOnly();
                PrintWriter out = new PrintWriter(
                        new BufferedWriter(new FileWriter(new File("journal.log.recovered"))))) {

            for (final SegmentId segmentId : fileStore.getSegmentIds()) {
                if (!segmentId.isDataSegmentId()) {
                    continue;
                }
                segmentId.getSegment().forEachRecord(new RecordConsumer() {

                    @Override
                    public void consume(int number, RecordType type, int offset) {
                        if (type != RecordType.NODE) {
                            return;
                        }
                        RecordId recordId = new RecordId(segmentId, number);
                        SegmentNodeState node = new SegmentNodeState(fileStore.getReader(), fileStore.getWriter(), null,
                                recordId);
                        try {
                            if (!node.hasChildNode("root")) {
                                return;
                            }
                            if (!node.hasChildNode("checkpoints")) {
                                return;
                            }
                        } catch (SegmentNotFoundException e) {
                            return;
                        }
                        out.printf("%s root %s\n", recordId.toString10(), System.currentTimeMillis());
                    }
                });
            }
        }

        return 0;
    }
}
