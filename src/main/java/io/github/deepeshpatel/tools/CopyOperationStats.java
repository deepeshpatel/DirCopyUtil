package io.github.deepeshpatel.tools;

import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.LongAdder;

/**
 * Tracks statistics for file copy operations.
 * Only classes in the same package can modify the statistics.
 */
public final class CopyOperationStats {
    private final LongAdder filesCopied = new LongAdder();
    private final LongAdder dataCopied = new LongAdder();
    private final Set<String> skippedFiles = ConcurrentHashMap.newKeySet();
    private final Set<String> failedFiles = ConcurrentHashMap.newKeySet();
    private final Set<String> errorSummaries = ConcurrentHashMap.newKeySet();
    private final Set<String> warningSummaries = ConcurrentHashMap.newKeySet();
    private final Set<String> overwrittenFiles = ConcurrentHashMap.newKeySet();
    private final long totalFiles;
    private final long totalDataSize;
    private long startTime;
    private long endTime = -1; // -1 indicates operation in progress

    // Package-private constructor
    CopyOperationStats(long totalFiles, long totalDataSize, List<String> skippedFiles) {
        this.totalFiles = totalFiles;
        this.totalDataSize = totalDataSize;
        this.skippedFiles.addAll(skippedFiles);
    }

    // =======================
    // Public getters
    // =======================
    public long getFilesCopied() { return filesCopied.sum(); }
    public long getDataCopied() { return dataCopied.sum(); }
    public Set<String> getSkippedFiles() { return Set.copyOf(skippedFiles); }
    public Set<String> getFailedFiles() { return Set.copyOf(failedFiles); }
    public Set<String> getErrorSummaries() { return Set.copyOf(errorSummaries); }
    public Set<String> getWarningSummaries() { return Set.copyOf(warningSummaries); }
    public Set<String> getOverwrittenFiles() { return Set.copyOf(overwrittenFiles); }
    public long getTotalFiles() { return totalFiles; }
    public long getTotalDataSize() { return totalDataSize; }
    public long getStartTime() { return startTime; }
    public long getEndTime() { return endTime; }
    public long getElapsedTimeMillis() {
        return endTime != -1 ? endTime - startTime : System.currentTimeMillis() - startTime;
    }

    public boolean isComplete() { return endTime != -1; }

    public double getCopySpeedMBps() {
        long elapsed = getElapsedTimeMillis();
        return elapsed > 0 ? (dataCopied.sum() / (1024.0 * 1024)) / (elapsed / 1000.0) : 0;
    }

    public double getProgressPercentage() {
        return totalDataSize > 0 ? (dataCopied.sum() * 100.0) / totalDataSize : 0;
    }

    // =======================
    // Package-private modifiers
    // =======================
    void markStarted() {
        this.startTime = System.currentTimeMillis();
    }

    void markCompleted() {
        this.endTime = System.currentTimeMillis();
    }

    void incrementFilesCopied() {
        filesCopied.increment();
    }

    void addCopiedData(long bytes) {
        dataCopied.add(bytes);
    }

    void addSkippedFile(String path) {
        skippedFiles.add(path);
    }

    void addFailedFile(String path) {
        failedFiles.add(path);
    }

    void addError(String error) {
        errorSummaries.add(error);
    }

    void addWarning(String warning) {
        warningSummaries.add(warning);
    }

    void addOverwrittenFile(String path) {
        overwrittenFiles.add(path);
    }

    // =======================
    // String representation
    // =======================
    @Override
    public String toString() {
        return "CopyOperationStats{" +
                "filesCopied=" + filesCopied +
                ", dataCopied=" + dataCopied +
                ", totalFiles=" + totalFiles +
                ", totalDataSize=" + totalDataSize +
                ", progress=" + String.format("%.1f%%", getProgressPercentage()) +
                '}';
    }

    public String getSummary() {
        return String.format(
                "Copy Operation Summary:\n" +
                        "-----------------------\n" +
                        "Files: %d/%d (%.1f%%)\n" +
                        "Data: %s/%s (%.1f%%)\n" +
                        "Speed: %.2f MB/s\n" +
                        "Elapsed: %d ms\n" +
                        "Skipped: %d | Failed: %d | Overwritten: %d\n" +
                        "Status: %s",
                getFilesCopied(), getTotalFiles(), getFilesCopied() * 100.0 / getTotalFiles(),
                formatSizeInMB(getDataCopied()), formatSizeInMB(getTotalDataSize()), getProgressPercentage(),
                getCopySpeedMBps(),
                getElapsedTimeMillis(),
                getSkippedFiles().size(), getFailedFiles().size(), getOverwrittenFiles().size(),
                isComplete() ? "COMPLETED" : "IN PROGRESS"
        );
    }

    private static String formatSizeInMB(long bytes) {
        double megabytes = bytes / (1024.0 * 1024.0);
        return String.format("%.2f MB", megabytes);
    }
}