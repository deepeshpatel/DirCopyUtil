package io.github.deepeshpatel.tools;

/**
 * Utility class to provides a simple progress call back method to print stats of copy operation.
 * User can use custom callback
 */
public class SimpleProgressCallback {
    /**
     * A simple progress callback that prints copy progress to the console.
     * @param stats the current statistics of the copy operation
     */
    public static void simpleProgressCallback(CopyOperationStats stats) {
        long currentTime = System.currentTimeMillis();
        long copiedFiles = stats.getFilesCopied();
        long currentDataCopied = stats.getDataCopied();
        double progress = stats.getTotalDataSize() > 0 ? (double) currentDataCopied / stats.getTotalDataSize() * 100 : 100.0;
        long elapsedTime = currentTime - stats.getStartTime();
        double currentSpeed = elapsedTime > 0 ? (double) currentDataCopied / elapsedTime * 1000 : 0;
        long remainingData = stats.getTotalDataSize() - currentDataCopied;
        long remainingTime = currentSpeed > 0 ? (long) (remainingData / currentSpeed) : 0;

        System.out.printf(
                "Progress: %.2f%% (%d/%d files copied) | Data Copied: %s/%s | Speed: %s/s | Elapsed: %d sec | Remaining: %d sec%n",
                progress, copiedFiles, stats.getTotalFiles(), formatSize(currentDataCopied), formatSize(stats.getTotalDataSize()),
                formatSize((long) currentSpeed), elapsedTime / 1000, remainingTime
        );
    }

    public static String formatSize(long size) {
        return SizeFormatter.formatSize(size);
    }

    private static class SizeFormatter {
        private static final long BYTES_PER_KB = 1024L;
        private static final long BYTES_PER_MB = BYTES_PER_KB * 1024L;
        private static final long BYTES_PER_GB = BYTES_PER_MB * 1024L;
        private static final String BYTE_FORMAT = "%d B";
        private static final String KB_FORMAT = "%.2f KB";
        private static final String MB_FORMAT = "%.2f MB";
        private static final String GB_FORMAT = "%.2f GB";

        /**
         * Formats a size in bytes into a human-readable string (e.g., "1.23 MB").
         *
         * @param size the size in bytes
         * @return a formatted string representing the size
         */
        public static String formatSize(long size) {
            if (size < BYTES_PER_KB) {
                return String.format(BYTE_FORMAT, size);
            } else if (size < BYTES_PER_MB) {
                return String.format(KB_FORMAT, size / (double) BYTES_PER_KB);
            } else if (size < BYTES_PER_GB) {
                return String.format(MB_FORMAT, size / (double) BYTES_PER_MB);
            } else {
                return String.format(GB_FORMAT, size / (double) BYTES_PER_GB);
            }
        }
    }

}
