package io.github.deepeshpatel.tools;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.stream.Stream;

import static io.github.deepeshpatel.tools.SimpleProgressCallback.formatSize;

/**
 * A thread-safe utility class for copying directories and their contents.
 * <p>
 * Features include:
 * - Multi-threaded copying for better performance
 * - Progress tracking and reporting
 * - Large file handling with configurable buffer sizes
 * - Comprehensive error handling and statistics
 * <p>
 * Multiple threads can call {@link #copyDirectory(Path, Path)} concurrently,
 * with each copy operation maintaining its own state (e.g., cancellation, statistics).
 */
public class DirectoryCopyUtil {
    private static final Logger logger = LoggerFactory.getLogger(DirectoryCopyUtil.class);
    private static final int DEFAULT_BUFFER_SIZE = 1024 * 1024 * 10; // 10MB
    private static final long LARGE_FILE_THRESHOLD = 1024 * 1024 * 20; // 20MB

    private final long progressUpdateInterval;
    private final int threadPoolSize;
    private final Set<CopyOption> copyOptions;
    private final int bufferSize;
    private final Consumer<Stats> progressCallback;

    /**
     * Statistics for a copy operation, tracking progress and outcomes.
     * <p>
     * All counters are thread-safe using atomic operations.
     */
    public static class Stats {
        private final AtomicInteger filesCopied;
        private final Set<String> skippedFiles = new HashSet<>();
        private final Set<String> failedFiles;
        private final Set<String> errorSummaries;
        private final Set<String> warningSummaries;
        private final Set<String> overwrittenFiles;
        private final long totalFiles;
        private final long totalDataSize;
        private final AtomicLong dataCopied;
        private long startTime;
        private long endTime;

        public Stats(long totalFiles, long totalDataSize) {
            this.filesCopied = new AtomicInteger(0);
            this.failedFiles = ConcurrentHashMap.newKeySet();
            this.errorSummaries = ConcurrentHashMap.newKeySet();
            this.warningSummaries = ConcurrentHashMap.newKeySet();
            this.overwrittenFiles = ConcurrentHashMap.newKeySet();
            this.totalFiles = totalFiles;
            this.totalDataSize = totalDataSize;
            this.dataCopied = new AtomicLong(0);
            this.startTime = 0;
            this.endTime = -1; // -1 represent that tak is incomplete
        }

        public int getFilesCopied() { return filesCopied.get(); }

        public Set<String> getSkippedFiles() { return skippedFiles; }
        public Set<String> getFailedFiles() { return failedFiles; }
        public Set<String> getErrorSummaries() { return errorSummaries; }
        public Set<String> getWarningSummaries() { return warningSummaries; }
        public Set<String> getOverwrittenFiles() { return overwrittenFiles; }
        public long getTotalFiles() { return totalFiles; }
        public long getTotalDataSize() { return totalDataSize; }
        public long getDataCopied() { return dataCopied.get(); }
        public long getStartTime() { return startTime; }

        @Override
        public String toString() {
            return "Stats{" +
                    "filesCopied=" + filesCopied +
                    ", skippedFiles=" + skippedFiles +
                    ", failedFiles=" + failedFiles +
                    ", errorSummaries=" + errorSummaries +
                    ", warningSummaries=" + warningSummaries +
                    ", overwrittenFiles=" + overwrittenFiles +
                    ", totalFiles=" + totalFiles +
                    ", totalDataSize=" + totalDataSize +
                    ", dataCopied=" + dataCopied +
                    ", startTime=" + startTime +
                    ", endTime=" + endTime +
                    '}';
        }

        public String summary() {
            return "Files Copied=" + filesCopied +
                    "\nFiles Skipped=" + skippedFiles.size() +
                    "\nFiles Failed=" + failedFiles.size() +
                    "\nFiles Overwritten=" + overwrittenFiles.size() +
                    "\nTotal Files=" + totalFiles +
                    "\nTotal Data=" + totalDataSize +
                    "\nData Copied=" + dataCopied +
                    "\nstartTime=" + startTime +
                    "\nendTime=" + endTime;
        }
    }

    /**
     * Represents a single copy operation with its associated statistics and cancellation control.
     * <p>
     * Each instance manages its own state and should not be shared across threads.
     */
    public class CopyOperation {
        private final Path source;
        private final Path target;
        private final List<Path> filesTobeCopies;
        private final Stats stats;
        private volatile boolean cancelled = false;
        private final ExecutorService executor = Executors.newFixedThreadPool(threadPoolSize+1); //+1 for startCopy task
        ScheduledExecutorService progressExecutor;
        private Future<?> future; // Tracks the async copy task

        private void shutdownExecutors() {
            if(progressExecutor != null) {
                progressExecutor.shutdown();
            }
            executor.shutdown();
        }

        private CopyOperation(Path source, Path target) throws IOException {
            assertInput(source, target);
            var preCalcStats = calculateTotalStats(source);
            this.stats = new Stats(preCalcStats.totalFiles, preCalcStats.totalDataSize);
            this.filesTobeCopies = preCalcStats.fileList;
            this.stats.skippedFiles.addAll(preCalcStats.skippedFiles);
            this.source = source;
            this.target = target;
            ensureDiskSpace(target, stats);
        }

        // Start the copy operation and store the Future
        private void startCopyAll() {
            progressExecutor = setupProgressReporter(stats);
            future = executor.submit(() -> {
                try {
                    copyAllFromSourceToTarget();
                } catch (IOException e) {
                    handleError("Error during copy operation: ",e, source);
                    Thread.currentThread().interrupt();
                } finally {
                    shutdownExecutors();
                }
            });
        }

        public void cancel() {
            cancelled = true;
            if (future != null) {
                future.cancel(true); // Interrupt the task if running
            }
            executor.shutdownNow(); // Forcibly stop all tasks
        }

        public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
            return executor.awaitTermination(timeout, unit);
        }

        public boolean isCancelled() {
            return cancelled || (future != null && future.isCancelled());
        }

        public Stats getStats() { return stats; }

        public boolean isTerminated() {
            return executor.isTerminated();
        }

        public boolean isDone() {
            return future.isDone();
        }

        public Future<?> getFuture() {
            return future;
        }


        private void copyAllFromSourceToTarget() throws IOException {
            logger.info("Total files to copy: {}", stats.getTotalFiles());
            logger.info("Total data size to copy: {}", formatSize(stats.getTotalDataSize()));

            if (isSameVolume(source, target) && copyOptions.contains(StandardCopyOption.COPY_ATTRIBUTES)) {
                String msg = "Note: Copying within the same volume, may use file cloning for speed";
                stats.getWarningSummaries().add(msg);
                logger.warn(msg);
            }

            if (Files.exists(target)) {
                try (Stream<Path> stream = Files.list(target)) {
                    if (stream.findAny().isPresent()) {
                        String msg = "Target directory " + target + " already contains files, some may be overwritten";
                        stats.getWarningSummaries().add(msg);
                        logger.warn("Target directory {} already contains files, some may be overwritten", target);
                    }
                }
            }

            stats.startTime = System.currentTimeMillis();
            copyFilesAndDirectories(source, target);
            stats.endTime = System.currentTimeMillis();
        }

        private void copyFilesAndDirectories(Path source, Path target) {
            CountDownLatch latch = new CountDownLatch((int)stats.totalFiles);

            for (Path item : filesTobeCopies) {
                if (isCancelled()) {
                    executor.shutdownNow();
                    break;
                }

                Path targetItem = target.resolve(source.relativize(item));
                logger.debug("Processing item: {} -> {}", item, targetItem);

                if (isDirectory(item)) {
                    logger.debug("Item {} is a directory", item);
                    createDirectory(targetItem);
                    continue;
                }

                executor.submit(() -> {
                    try {
                        if (Files.isSymbolicLink(item)) {
                            logger.debug("Item {} is a symlink, copying as symlink", item);
                            copySymbolicLink(item, targetItem);
                        } else if (MacOSAliasCopyUtil.isMacOSAlias(item)) {
                            logger.debug("Item {} is a macOS Alias, copying as alias", item);
                            MacOSAliasCopyUtil.copyMacOsAlias(item, targetItem, this.source, this.target,
                                    copyOptions, p -> recordFileCopyStats(p, 0));
                        } else {
                            logger.debug("Item {} is not a symlink or alias, copying as file", item);
                            copySingleFile(item, targetItem);
                        }
                    } catch (Exception e) {
                        handleError("Error copying item", e, item);
                    } finally {
                        latch.countDown();
                    }
                });
            }

            try {
                latch.await();
                if (!isCancelled()) {
                    progressCallback.accept(stats);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                handleError("Copy operation interrupted: ", e, null);
            }
        }

        private void createDirectory(Path targetItem) {
            try {
                Files.createDirectories(targetItem);
                logger.debug("Created directory: {}", targetItem);
            } catch (IOException e) {
                String message = "Error creating directory " + targetItem + ": " + e.getMessage();
                stats.getErrorSummaries().add(message);
                stats.getFailedFiles().add(targetItem.toString());
                logger.error(message);
            }
        }

        private void copySymbolicLink(Path source, Path target) throws IOException {
            if (isCancelled()) return;

            Path linkTarget = Files.readSymbolicLink(source);
            Path sourceBase = this.source;
            Path targetBase = this.target;

            logger.debug("Symlink {} points to {}", source, linkTarget);

            Path resolvedPath = linkTarget.isAbsolute() ? linkTarget : source.getParent().resolve(linkTarget).normalize();
            logger.debug("Resolved path: {}", resolvedPath);

            Path finalLinkTarget;
            if (resolvedPath.startsWith(sourceBase)) {
                Path relativePath = sourceBase.relativize(resolvedPath);
                finalLinkTarget = targetBase.resolve(relativePath);
                logger.debug("Internal symlink, rebasing to {}", finalLinkTarget);
            } else {
                finalLinkTarget = linkTarget;
                logger.debug("External symlink, preserving as {}", finalLinkTarget);
            }

            try {
                Files.createSymbolicLink(target, finalLinkTarget);
                logger.debug("Created symlink {} -> {}", target, finalLinkTarget);
            } catch (IOException e) {
                logger.error("Failed to create symlink {} -> {}", target, finalLinkTarget, e);
                throw e; // Ensure the exception propagates
            }
            recordFileCopyStats(source, 0);
        }


        private void copySingleFile(Path source, Path target) {
            if (isCancelled()) return;

            try {
                // Check for existing file
                if (Files.exists(target)) {
                    if (copyOptions.contains(StandardCopyOption.REPLACE_EXISTING)) {
                        stats.getOverwrittenFiles().add(target.toString());
                        logger.warn("File will be overwritten: {}", target);
                    } else {
                        stats.getFailedFiles().add(target.toString());
                        stats.getErrorSummaries().add("File already exist:" + target);
                        return;
                    }
                }

                long fileSize = Files.size(source);

                // Handle large files vs normal files
                if (fileSize > LARGE_FILE_THRESHOLD) {
                    copyLargeFile(source, target);
                } else {
                    copySmallFile(source, target);
                }
            } catch (Exception e) {
                handleError("Error copying file", e, source);
            }
        }

        private void copySmallFile(Path source, Path target)  {
            if (isCancelled()) return;
            try {
                Files.copy(source, target, copyOptions.toArray(new CopyOption[0]));
                recordFileCopyStats(source, Files.size(source));
            }catch(IOException e) {
                handleError("Failed to copy", e, source);
            }
        }

        private void copyLargeFile(Path source, Path target) throws IOException {
            if (isCancelled()) return;

            long totalBytesCopied = 0;
            boolean copyCompleted = false;
            try (InputStream in = Files.newInputStream(source);
                 OutputStream out = Files.newOutputStream(target)) {
                byte[] buffer = new byte[bufferSize];
                int bytesRead;
                long fileSize = Files.size(source);

                while ((bytesRead = in.read(buffer)) != -1 && !isCancelled()) {
                    if (!Files.exists(source)) {
                        throw new NoSuchFileException("Source file deleted during copy: " + source);
                    }
                    out.write(buffer, 0, bytesRead);
                    totalBytesCopied += bytesRead;
                    stats.dataCopied.addAndGet(bytesRead);
                }

                if (isCancelled()) {
                    out.close();
                    Files.deleteIfExists(target);
                    String msg = "Cancelled mid-copy: " + source;
                    stats.getWarningSummaries().add(msg);
                    stats.getFailedFiles().add(source.toString());
                    logger.warn(msg);
                    return;
                }

                if (totalBytesCopied != fileSize) {
                    throw new IOException("Incomplete copy of " + source + ": expected " + fileSize + ", copied " + totalBytesCopied);
                }

                if (copyOptions.contains(StandardCopyOption.COPY_ATTRIBUTES)) {
                    Files.setLastModifiedTime(target, Files.getLastModifiedTime(source));
                }

                copyCompleted = true;
                stats.filesCopied.incrementAndGet();
                logger.debug("Copied large file: {}", source);
            } catch (Exception e) {
                Files.deleteIfExists(target);

                handleError("Failed to copy large file", e, source);
            } finally {
                if (!copyCompleted && !isCancelled()) {
                    handleError("Large file copy failed: ", new Exception("Incomplete copy"), source);
                }
            }
        }

        private void recordFileCopyStats(Path source, long fileSize) {
            stats.dataCopied.addAndGet(fileSize);
            stats.filesCopied.incrementAndGet();
            logger.debug("Copied file: {}", source);
        }

        private void handleError(String message, Exception e, Path path) {
            String errMsg = String.format("%s: %s [%s] - Path: %s",
                    message,
                    e.getMessage(),
                    e.getClass().getSimpleName(),
                    path);
            logger.error(errMsg, e); // Log full exception stack trace
            stats.getErrorSummaries().add(errMsg);
            if (path != null) {
                stats.getFailedFiles().add(path.toString());
            }
        }
    }

    /**
     * Builder for constructing {@link DirectoryCopyUtil} instances with custom settings.
     * <p>
     * Example:
     * <pre>{@code
     * DirectoryCopyUtil util = new DirectoryCopyUtil.Builder()
     *     .threadPoolSize(16)
     *     .bufferSize(20 * 1024 * 1024)
     *     .progressCallback(stats -> {...})
     *     .build();
     * }</pre>
     */
    public static class Builder {
        private long progressUpdateInterval = 2;
        private int threadPoolSize = Runtime.getRuntime().availableProcessors() * 8;
        private Set<CopyOption> copyOptions = Set.of(StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.COPY_ATTRIBUTES);
        private int bufferSize = DEFAULT_BUFFER_SIZE;
        private Consumer<Stats> progressCallback = stats -> {};

        public Builder progressUpdateInterval(long interval) {
            this.progressUpdateInterval = interval;
            return this;
        }

        public Builder threadPoolSize(int size) {
            this.threadPoolSize = size;
            return this;
        }

        public Builder copyOptions(Set<CopyOption> options) {
            this.copyOptions = options;
            return this;
        }

        public Builder bufferSize(int size) {
            this.bufferSize = size;
            return this;
        }

        /**
         * Sets the progress callback that will be invoked periodically during copy operations.
         *
         * @param callback the callback to receive progress updates (can be null to disable)
         * @return this builder
         */
        public Builder progressCallback(Consumer<Stats> callback) {
            this.progressCallback = callback != null ? callback : stats -> {};
            return this;
        }

        public DirectoryCopyUtil build() {
            return new DirectoryCopyUtil(progressUpdateInterval, threadPoolSize, copyOptions, bufferSize, progressCallback);
        }
    }

    protected DirectoryCopyUtil(long progressUpdateInterval, int threadPoolSize,
                                Set<CopyOption> copyOptions, int bufferSize,
                                Consumer<Stats> progressCallback) {
        if (progressUpdateInterval <= 0) {
            throw new IllegalArgumentException("Progress update interval must be positive");
        }
        if (threadPoolSize <= 0) {
            throw new IllegalArgumentException("Thread pool size must be >=1");
        }
        if (copyOptions == null) {
            throw new IllegalArgumentException("Copy options must not be null");
        }
        if (bufferSize <= 0) {
            throw new IllegalArgumentException("Buffer size must be positive");
        }
        this.progressUpdateInterval = progressUpdateInterval;
        this.threadPoolSize = threadPoolSize;
        this.copyOptions = copyOptions;
        this.bufferSize = bufferSize;
        this.progressCallback = progressCallback;
    }

    /**
     * Initiates a directory copy operation using the progress callback configured in the builder.
     *
     * @param source the source directory to copy from
     * @param target the target directory to copy to
     * @return the CopyOperation instance immediately
     * @throws IOException if an I/O error occurs during initialization
     */
    public CopyOperation copyDirectory(Path source, Path target) throws IOException {
        var copyOperation = new CopyOperation(source, target);
        copyOperation.startCopyAll();
        return copyOperation;
    }

    private void assertInput(Path source, Path target) throws IOException {
        validateSourceDirectory(source);
        validateTargetDirectory(source, target);
    }

    private void validateSourceDirectory(Path source) throws IOException {
        if (!Files.exists(source) || !Files.isDirectory(source)) {
            throw new IllegalArgumentException("Source directory does not exist or is not a directory: " + source);
        }
        if (!Files.isReadable(source)) {
            throw new IOException("Read permission denied for source directory: " + source);
        }
    }

    private void validateTargetDirectory(Path source, Path target) throws IOException {
        Files.createDirectories(target);
        if (Files.isSameFile(source, target)) {
            throw new IllegalArgumentException("Target directory cannot be the same as the source directory: " + target);
        }
        if (target.startsWith(source)) {
            throw new IllegalArgumentException("Target directory cannot be a subdirectory of the source directory: " + target);
        }
        if (Files.exists(target) && !Files.isWritable(target)) {
            throw new IOException("Write permission denied for target directory: " + target);
        }
    }

    private void ensureDiskSpace(Path target, Stats stats) throws IOException {
        FileStore fileStore = Files.getFileStore(target);
        long freeSpace = fileStore.getUsableSpace();

        if (stats.totalDataSize > freeSpace) {
            logger.warn("Not enough disk space on target. Copy operation might fail");
            throw new IOException("Not enough disk space on target: " + target);
        } else if (stats.totalDataSize == 0) {
            stats.getWarningSummaries().add("Warning: Source directory is empty.");
            logger.warn("Source directory is empty");
        }
    }

    private boolean isSameVolume(Path source, Path target) throws IOException {
        return Files.getFileStore(source).equals(Files.getFileStore(target));
    }

    private ScheduledExecutorService setupProgressReporter(Stats stats) {
        ScheduledExecutorService progressExecutor = Executors.newSingleThreadScheduledExecutor();
        progressExecutor.scheduleAtFixedRate(
                () -> progressCallback.accept(stats),
                progressUpdateInterval,
                progressUpdateInterval,
                TimeUnit.SECONDS
        );
        return progressExecutor;
    }

    /**
     * Checks if a path is a real directory (not a symlink)
     * @param path the path to check
     * @return true if it's a directory and not a symlink
     */
    private static boolean isDirectory(Path path) {
        return !Files.isSymbolicLink(path) && Files.isDirectory(path);
    }

    private StatsWithFileList calculateTotalStats(Path source) throws IOException {
        StatsCollector visitor = new StatsCollector();
        Files.walkFileTree(source, visitor);
        return visitor.getStats();
    }

    private static class StatsCollector extends SimpleFileVisitor<Path> {
        private long totalFiles = 0;
        private long totalDataSize = 0;
        private final List<Path> fileList = new ArrayList<>();
        private final List<String> skippedFiles = new ArrayList<>();

        @Override
        public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) {

                try {
                    if (!Files.isReadable(file)) {
                        throw new AccessDeniedException("File not readable");
                    }

                    fileList.add(file); //add in all cases dir, link file
                    if(!isDirectory(file)) {
                        totalFiles++; //file count increase only in case of file and link
                    }

                    if (!isDirectory(file) && !Files.isSymbolicLink(file)) {
                        totalDataSize += Files.size(file); // data only in case of actual file
                    }

                } catch (Exception e) {
                    skippedFiles.add(file.toString());
                    logger.error("Skipping inaccessible file: {} - {}", file, e.getMessage());
                }
            return FileVisitResult.CONTINUE;
        }

        @Override
        public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) {
            fileList.add(dir); // Add directories to the list
            return FileVisitResult.CONTINUE;
        }

        @Override
        public FileVisitResult visitFileFailed(Path file, IOException exc) {
            return FileVisitResult.CONTINUE; // Skip and continue traversal
        }

        public StatsWithFileList getStats() {
            return new StatsWithFileList(totalFiles, totalDataSize, fileList, skippedFiles);
        }
    }

    public record StatsWithFileList(long totalFiles, long totalDataSize, List<Path> fileList, List<String> skippedFiles) { }
    public long getProgressUpdateInterval() { return progressUpdateInterval; }
    public int getThreadPoolSize() { return threadPoolSize; }
    public Set<CopyOption> getCopyOptions() { return copyOptions; }
    public int getBufferSize() { return bufferSize; }

}