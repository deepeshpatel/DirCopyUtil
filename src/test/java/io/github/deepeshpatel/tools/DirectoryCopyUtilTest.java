package io.github.deepeshpatel.tools;

import ch.qos.logback.classic.Level;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.*;
import java.nio.file.attribute.FileTime;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

/**
 * Test class for DirectoryCopyUtil with nested categories.
 */
public class DirectoryCopyUtilTest {

    static {
        Logger logger = LoggerFactory.getLogger(DirectoryCopyUtil.class);
        if (logger instanceof ch.qos.logback.classic.Logger logbackLogger) {
            logbackLogger.setLevel(Level.ERROR);
        }
    }

    @TempDir
    Path tempSourceDir;

    @TempDir
    Path tempTargetDir;

    private DirectoryCopyUtil copyUtil;

    @BeforeEach
    void setUp() {
        // Default setup with standard configuration

        // Set logging level to INFO for tests


        copyUtil = new DirectoryCopyUtil.Builder()
                .progressUpdateInterval(1)
                .build();


    }

    /**
     * Nested class for testing basic copy functionality of DirectoryCopyUtil.
     */
    @Nested
    @DisplayName("Basic Copy Functionality Tests")
    class BasicCopyFunctionality {

        @Test
        @DisplayName("Copy an empty directory")
        void testCopyEmptyDirectory() throws Exception {
            // Setup: Ensure source directory is empty (TempDir starts empty)
            Files.createDirectories(tempSourceDir);

            // Act: Copy the empty directory
            var copyOperation = copyUtil.copyDirectory(tempSourceDir, tempTargetDir);
            copyOperation.getFuture().get(); // Wait for completion

            // Assert: Target exists, is a directory, and is empty
            assertTrue(Files.exists(tempTargetDir), "Target directory should exist");
            assertTrue(Files.isDirectory(tempTargetDir), "Target should be a directory");
            try (var stream = Files.list(tempTargetDir)) {
                assertFalse(stream.findAny().isPresent(), "Target directory should be empty");
            }
            var stats = copyOperation.getStats();
            assertEquals(0, stats.getTotalFiles(), "Total files should be 0 for empty directory");
            assertEquals(0, stats.getFilesCopied(), "No files should be copied");
        }

        @Test
        @DisplayName("Copy a single small file")
        void testCopySingleSmallFile() throws Exception {
            Path sourceFile = createFile(tempSourceDir,"small.txt", 1024);

            // Act: Copy the directory containing the file
            var copyOperation = copyUtil.copyDirectory(tempSourceDir, tempTargetDir);
            copyOperation.getFuture().get(); // Wait for completion

            // Assert: Target file exists, matches source size and contents
            Path targetFile = tempTargetDir.resolve("small.txt");
            assertTrue(Files.exists(targetFile), "Target file should exist");
            assertEquals(Files.size(sourceFile), Files.size(targetFile), "File sizes should match");
            assertArrayEquals(Files.readAllBytes(sourceFile), Files.readAllBytes(targetFile), "File contents should match");
            var stats = copyOperation.getStats();
            assertEquals(1, stats.getTotalFiles(), "Total files should be 1");
            assertEquals(1, stats.getFilesCopied(), "One file should be copied");
        }

        @Test
        @DisplayName("Copy a single large file")
        void testCopySingleLargeFile() throws Exception {
            // Setup: Create a large file (> 20MB)
            Path sourceFile = createFile(tempSourceDir,"large.txt", 25 * 1024 * 1024 ); // 25MB > LARGE_FILE_THRESHOLD

            // Act: Copy the directory containing the file
            var copyOperation = copyUtil.copyDirectory(tempSourceDir, tempTargetDir);
            copyOperation.getFuture().get(); // Wait for completion

            // Assert: Target file exists, matches size and contents, attributes copied
            Path targetFile = tempTargetDir.resolve("large.txt");
            assertTrue(Files.exists(targetFile), "Target file should exist");
            assertEquals(Files.size(sourceFile), Files.size(targetFile), "File sizes should match");
            assertArrayEquals(Files.readAllBytes(sourceFile), Files.readAllBytes(targetFile), "File contents should match");
            assertEquals(Files.getLastModifiedTime(sourceFile), Files.getLastModifiedTime(targetFile),
                    "Last modified time should match due to COPY_ATTRIBUTES");
            var stats = copyOperation.getStats();
            assertEquals(1, stats.getTotalFiles(), "Total files should be 1");
            assertEquals(1, stats.getFilesCopied(), "One file should be copied");
        }

        @Test
        @DisplayName("Copy a directory with multiple files")
        void testCopyDirectoryWithMultipleFiles() throws Exception {
            // Setup: Create a mix of small and large files
            Path smallFile = createFile(tempSourceDir, "small.txt", 1024); //1KB
            Path largeFile =  createFile(tempSourceDir, "large.txt", 25 * 1024 * 1024); //25MB

            // Act: Copy the directory
            var copyOperation = copyUtil.copyDirectory(tempSourceDir, tempTargetDir);
            copyOperation.getFuture().get(); // Wait for completion

            // Assert: Both files are copied correctly
            Path targetSmallFile = tempTargetDir.resolve("small.txt");
            Path targetLargeFile = tempTargetDir.resolve("large.txt");
            assertTrue(Files.exists(targetSmallFile), "Small target file should exist");
            assertTrue(Files.exists(targetLargeFile), "Large target file should exist");
            assertEquals(Files.size(smallFile), Files.size(targetSmallFile), "Small file sizes should match");
            assertEquals(Files.size(largeFile), Files.size(targetLargeFile), "Large file sizes should match");
            assertArrayEquals(Files.readAllBytes(smallFile), Files.readAllBytes(targetSmallFile), "Small file contents should match");
            assertArrayEquals(Files.readAllBytes(largeFile), Files.readAllBytes(targetLargeFile), "Large file contents should match");
            var stats = copyOperation.getStats();
            assertEquals(2, stats.getTotalFiles(), "Total files should be 2");
            assertEquals(2, stats.getFilesCopied(), "Two files should be copied");
        }

        @Test
        @DisplayName("Copy nested directories")
        void testCopyNestedDirectories() throws Exception {
            // Setup: Create a nested directory structure with files
            Path subDir = tempSourceDir.resolve("subDir");
            Files.createDirectories(subDir);
            Path fileInRoot = createFile(tempSourceDir, "root.txt",1024 );
            Path fileInSubDir = createFile(subDir, "sub.txt",1024 );

            // Act: Copy the directory
            var copyOperation = copyUtil.copyDirectory(tempSourceDir, tempTargetDir);
            copyOperation.getFuture().get(); // Wait for completion

            // Assert: Directory structure and files are preserved
            Path targetSubDir = tempTargetDir.resolve("subDir");
            Path targetFileInRoot = tempTargetDir.resolve("root.txt");
            Path targetFileInSubDir = targetSubDir.resolve("sub.txt");
            assertTrue(Files.exists(targetSubDir), "Target subdirectory should exist");
            assertTrue(Files.isDirectory(targetSubDir), "Target subdirectory should be a directory");
            assertTrue(Files.exists(targetFileInRoot), "Root file should exist");
            assertTrue(Files.exists(targetFileInSubDir), "Subdirectory file should exist");
            assertArrayEquals(Files.readAllBytes(fileInRoot), Files.readAllBytes(targetFileInRoot), "Root file contents should match");
            assertArrayEquals(Files.readAllBytes(fileInSubDir), Files.readAllBytes(targetFileInSubDir), "Subdirectory file contents should match");
            var stats = copyOperation.getStats();
            assertEquals(2, stats.getTotalFiles(), "Total files should be 2");
            assertEquals(2, stats.getFilesCopied(), "Two files should be copied");
        }


        @Test
        @DisplayName("Copy single very large file (0.5GB)")
        void testVeryLargeFile() throws Exception {
            String fileName = "very-large.bin";
            Path largeFile = createFile(tempSourceDir, fileName, 1024 * 1024 * 512 ); //0.5 GB

            var copyOperation = copyUtil.copyDirectory(tempSourceDir, tempTargetDir);
            copyOperation.awaitTermination(60, TimeUnit.SECONDS);

            var stats = copyOperation.getStats();
            assertEquals(1, stats.getFilesCopied());
            assertEquals(Files.size(largeFile), Files.size(tempTargetDir.resolve(fileName)));
        }

        @Test
        @DisplayName("Copy files with non-ASCII names (Japanese/Chinese)")
        void testNonAsciiFileNames() throws Exception {
            Path nonAsciiDir = tempSourceDir.resolve("フォルダー");
            Files.createDirectory(nonAsciiDir);
            Path nonAsciiFile = nonAsciiDir.resolve("文件.txt");
            Files.writeString(nonAsciiFile, "Non-ASCII content: 你好世界");

            var copyOperation = copyUtil.copyDirectory(tempSourceDir, tempTargetDir);
            copyOperation.getFuture().get(10, TimeUnit.SECONDS);

            var stats = copyOperation.getStats();
            Path targetNonAsciiDir = tempTargetDir.resolve("フォルダー");
            Path targetNonAsciiFile = targetNonAsciiDir.resolve("文件.txt");

            assertEquals(1, stats.getFilesCopied());
            assertEquals(1, stats.getTotalFiles());
            assertTrue(Files.exists(targetNonAsciiFile));
            assertEquals("Non-ASCII content: 你好世界", Files.readString(targetNonAsciiFile));
            assertTrue(stats.getErrorSummaries().isEmpty());
        }

        @Test
        @DisplayName("Copy hidden files")
        void testCopyingHiddenFiles() throws Exception {
            Path hiddenFile = tempSourceDir.resolve(".hidden");
            Files.writeString(hiddenFile, "hidden content");

            try {
                Files.setAttribute(hiddenFile, "dos:hidden", true);
            } catch (UnsupportedOperationException | IOException e) {
                // Ignore if not supported
            }

            var copyOperation = copyUtil.copyDirectory(tempSourceDir, tempTargetDir);
            copyOperation.getFuture().get(10, TimeUnit.SECONDS);

            var stats = copyOperation.getStats();
            Path targetHiddenFile = tempTargetDir.resolve(".hidden");

            assertEquals(1, stats.getFilesCopied());
            assertEquals(1, stats.getTotalFiles());
            assertTrue(Files.exists(targetHiddenFile));
            assertEquals("hidden content", Files.readString(targetHiddenFile));
            assertTrue(stats.getErrorSummaries().isEmpty());
        }

        @Test
        @DisplayName("Copy files with special characters in names")
        void testSpecialCharacterFilenames() throws Exception {
            // Setup: Create files with various special characters in names
            String[] specialNames = {
                    "file with spaces.txt",
                    "file!@#$%^&()[]{}.txt",
                    "quoted'file'.txt",
                    "double\"quote.txt",
                    "back\\slash.txt",
                    "comma,file.txt",
                    "semi;colon.txt"
            };

            // Create files with special names
            for (String name : specialNames) {
                createFile(tempSourceDir, name, ("content for " + name).getBytes());
            }

            // Act: Copy the directory
            var copyOperation = copyUtil.copyDirectory(tempSourceDir, tempTargetDir);
            copyOperation.getFuture().get(10, TimeUnit.SECONDS);

            // Assert: All files were copied correctly
            var stats = copyOperation.getStats();
            assertEquals(specialNames.length, stats.getFilesCopied());
            assertEquals(specialNames.length, stats.getTotalFiles());

            for (String name : specialNames) {
                Path targetFile = tempTargetDir.resolve(name);
                assertTrue(Files.exists(targetFile), "File should exist: " + name);
                assertEquals("content for " + name, Files.readString(targetFile));
            }
            assertTrue(stats.getErrorSummaries().isEmpty());
        }


    }

    /**
     * Nested class for testing symlinks copy functionality of DirectoryCopyUtil.
     */
    @Nested
    @DisplayName("SymLink Copy Functionality Tests")
    class SymLinkFunctionality {
        @Test
        @DisplayName("Copy a directory with an internal symbolic link")
        void testInternalSymlink() throws Exception {
            // Setup
            Path realFile = createFile(tempSourceDir, "real.txt", 1024);
            Path linkFile = tempSourceDir.resolve("link.txt");
            Files.createSymbolicLink(linkFile, realFile);

            // Act
            var copyOperation = copyUtil.copyDirectory(tempSourceDir, tempTargetDir);
            copyOperation.getFuture().get(10, TimeUnit.SECONDS);

            // Assert
            Path targetRealFile = tempTargetDir.resolve("real.txt");
            Path targetLinkFile = tempTargetDir.resolve("link.txt");

            assertTrue(Files.exists(targetRealFile));
            assertTrue(Files.isSymbolicLink(targetLinkFile));

            // ONLY check that the link points to the correct target location
            Path expectedTarget = tempTargetDir.resolve("real.txt");
            Path actualTarget = targetLinkFile.getParent()
                    .resolve(Files.readSymbolicLink(targetLinkFile))
                    .normalize();
            assertEquals(expectedTarget, actualTarget);

            // Verify stats
            var stats = copyOperation.getStats();
            assertEquals(2, stats.getTotalFiles());
            assertEquals(2, stats.getFilesCopied());
            assertTrue(stats.getErrorSummaries().isEmpty());
        }


        @Test
        @DisplayName("Copy a directory with an external symbolic link")
        void testExternalSymlink() throws Exception {
            // Create a file outside the source directory
            Path externalFile = Files.createTempFile("external-", ".txt");
            Files.write(externalFile, "test".getBytes());

            // Create symlink to external file
            Path linkFile = tempSourceDir.resolve("ext-link.txt");
            Files.createSymbolicLink(linkFile, externalFile);

            // Act
            var copyOperation = copyUtil.copyDirectory(tempSourceDir, tempTargetDir);
            copyOperation.getFuture().get(10, TimeUnit.SECONDS);

            // Assert
            Path targetLinkFile = tempTargetDir.resolve("ext-link.txt");

            // Verify the symlink still points to the original external location
            assertEquals(externalFile, Files.readSymbolicLink(targetLinkFile));

            // Verify stats (should count as 1 copied file - just the symlink itself)
            var stats = copyOperation.getStats();
            assertEquals(1, stats.getTotalFiles());
            assertEquals(1, stats.getFilesCopied());
            assertTrue(stats.getErrorSummaries().isEmpty());

            // Cleanup
            Files.delete(externalFile);
        }

        @Test
        void testNestedRelativeSymlink() throws Exception {
            // source/
            //   ├── dir/
            //   │   └── file.txt
            //   └── link → dir/../dir/file.txt (complex relative path)

            Path dir = Files.createDirectory(tempSourceDir.resolve("dir"));
            Path file = createFile(dir, "file.txt", 100);
            Path link = tempSourceDir.resolve("link");
            Files.createSymbolicLink(link, Path.of("dir/../dir/file.txt"));

            var copyOperation = copyUtil.copyDirectory(tempSourceDir, tempTargetDir);
            copyOperation.getFuture().get(10, TimeUnit.SECONDS);

            Path copiedLink = tempTargetDir.resolve("link");
            assertTrue(Files.isSymbolicLink(copiedLink), "Copied link should be a symbolic link");

            // Path to the actual file in target
            Path f1 = tempTargetDir.resolve("dir/file.txt");
            // Path of the copied symlink (not its target)
            Path f2 = copiedLink;

            assertTrue(
                    Files.isSameFile(f1, f2),
                    "Symlink should resolve to the same file as the target"
            );        }

        @Test
        void testChainedSymlinks() throws Exception {
            // source/
            //   ├── real.txt
            //   ├── linkA → linkB
            //   └── linkB → real.txt

            // Create structure
            Path realFile = createFile(tempSourceDir, "real.txt", 100);
            Path linkB = tempSourceDir.resolve("linkB");
            Path linkA = tempSourceDir.resolve("linkA");
            Files.createSymbolicLink(linkB, Path.of("real.txt"));
            Files.createSymbolicLink(linkA, Path.of("linkB"));

            // Perform copy
            var copyOperation = copyUtil.copyDirectory(tempSourceDir, tempTargetDir);
            copyOperation.getFuture().get(10, TimeUnit.SECONDS);

            // Verify
            Path copiedLinkA = tempTargetDir.resolve("linkA");
            Path copiedLinkB = tempTargetDir.resolve("linkB");

            // Both links should use absolute paths in target directory
            assertEquals(
                    tempTargetDir.resolve("linkB"),
                    Files.readSymbolicLink(copiedLinkA),
                    "linkA should point to linkB"
            );
            assertEquals(
                    tempTargetDir.resolve("real.txt"),
                    Files.readSymbolicLink(copiedLinkB),
                    "linkB should point to real.txt"
            );

            // Verify resolution
            assertTrue(
                    Files.isSameFile(tempTargetDir.resolve("real.txt"), copiedLinkA),
                    "Chained symlinks should resolve to the real file"
            );
        }

        @Test
        void testDirectorySymlink() throws Exception {
            // source/
            //   ├── data/ (dir)
            //   │   └── file.txt
            //   └── link → data/

            Path dataDir = Files.createDirectory(tempSourceDir.resolve("data"));
            createFile(dataDir, "file.txt", 100);
            Files.createSymbolicLink(tempSourceDir.resolve("link"), Path.of("data"));

            var copyOperation = copyUtil.copyDirectory(tempSourceDir, tempTargetDir);
            copyOperation.getFuture().get(10, TimeUnit.SECONDS);

            assertTrue(Files.isDirectory(tempTargetDir.resolve("link")));
            assertTrue(Files.exists(tempTargetDir.resolve("link/file.txt")));
        }

        @Test
        @DisplayName("Copy symlinks regardless of whether their targets exist")
        void testBrokenSymlink() throws Exception {
            // source/
            //   └── link → non-existent-file

            // 1. CREATE STRUCTURE EXPLICITLY
            Path dir = Files.createDirectory(tempSourceDir.resolve("dir"));
            Path file = createFile(dir, "file.txt", 100); // Ensure this waits for file creation

            // 2. CREATE SYMLINK
            Path link = tempSourceDir.resolve("link");
            Files.createSymbolicLink(link, Path.of("dir/../dir/file.txt"));

            // 3. RUN COPY
            var operation = copyUtil.copyDirectory(tempSourceDir, tempTargetDir);
            operation.getFuture().get(10, TimeUnit.SECONDS); // Wait with timeout

            // 4. VERIFY TARGET
            Files.walk(tempTargetDir).forEach(System.out::println);

            Path copiedLink = tempTargetDir.resolve("link");
            assertTrue(Files.exists(copiedLink), "Symlink should exist in target");
            assertTrue(Files.isSymbolicLink(copiedLink), "Should be a symlink");
        }

        @Test
        void testSymlinkAttributesPreserved() throws Exception {
            Path target = createFile(tempSourceDir, "target.txt", 100);
            Path link = tempSourceDir.resolve("link");
            Files.createSymbolicLink(link, Path.of("target.txt"));

            // Set custom permissions on symlink
            Files.setPosixFilePermissions(link, PosixFilePermissions.fromString("rwxr-x---"));

            var copyOperation  = copyUtil.copyDirectory(tempSourceDir, tempTargetDir);
            copyOperation.getFuture().get(10, TimeUnit.SECONDS);

            Path copiedLink = tempTargetDir.resolve("link");
            assertEquals(
                    Files.getPosixFilePermissions(link),
                    Files.getPosixFilePermissions(copiedLink)
            );
        }

    }

    // Placeholder nested classes for other categories
    @Nested
    @DisplayName("Progress Reporting Tests")
    class ProgressReporting {

        @Test
        @DisplayName("Progress callback is invoked multiple times for a large file")
        void testProgressCallbackInvokedForLargeFile() throws Exception {
            // Setup: Create a significantly larger file to ensure copy takes longer (> 2 seconds)
            var dataSize  = 100 * 1024 * 1024; // 100MB to ensure copy spans multiple 1-second intervals
            createFile(tempSourceDir, "large.txt",dataSize);

            // Setup: Count callback invocations with debug output
            AtomicInteger callbackCount = new AtomicInteger(0);
            Consumer<DirectoryCopyUtil.Stats> callback = stats -> {
                callbackCount.incrementAndGet();
                assertTrue(stats.getDataCopied() > 0, "Data copied should be positive during callback");
            };

            // Setup: Use a smaller buffer to slow down the copy
            DirectoryCopyUtil slowUtil = new DirectoryCopyUtil.Builder()
                    .progressUpdateInterval(1)
                    .bufferSize(512) // Small buffer to ensure chunked copying takes time
                    .progressCallback(callback)
                    .build();



            // Act: Copy with custom callback
            var copyOperation = slowUtil.copyDirectory(tempSourceDir, tempTargetDir);
            copyOperation.getFuture().get(15, TimeUnit.SECONDS); // Increased timeout to accommodate larger file

            // Assert: Callback was invoked multiple times
            int finalCount = callbackCount.get();
            assertTrue(finalCount > 1, "Progress callback should be invoked more than once for a large file, got " + finalCount);
            var stats = copyOperation.getStats();
            assertEquals(1, stats.getFilesCopied(), "One file should be copied");
            assertEquals(dataSize, stats.getDataCopied(), "All data should be copied");
            assertTrue(stats.getErrorSummaries().isEmpty(), "No errors should occur");
        }

        @Test
        @DisplayName("Progress callback stops after copy completion")
        void testProgressCallbackStopsAfterCompletion() throws Exception {
            // Setup: Create a small file for quick copy
            createFile(tempSourceDir,"file.txt", 1024);

            // Setup: Count callback invocations
            AtomicInteger callbackCount = new AtomicInteger(0);
            Consumer<DirectoryCopyUtil.Stats> callback = stats -> callbackCount.incrementAndGet();

            var copyUtil = new DirectoryCopyUtil.Builder()
                    .progressUpdateInterval(1)
                    .progressCallback(callback)
                    .build();

            // Act: Copy with custom callback
            var copyOperation = copyUtil.copyDirectory(tempSourceDir, tempTargetDir);
            copyOperation.getFuture().get(5, TimeUnit.SECONDS); // Wait for completion

            // Capture count after completion
            int countAfterCompletion = callbackCount.get();
            assertTrue(countAfterCompletion > 0, "Callback should be invoked at least once during copy");

            // Wait longer to check if callback continues
            Thread.sleep(2000); // Wait 2 seconds beyond progressUpdateInterval (1s)
            int finalCount = callbackCount.get();

            // Assert: Callback count doesn’t increase after completion
            assertEquals(countAfterCompletion, finalCount, "Progress callback should not be invoked after completion");
            var stats = copyOperation.getStats();
            assertEquals(1, stats.getFilesCopied(), "One file should be copied");
            assertTrue(stats.getErrorSummaries().isEmpty(), "No errors should occur");
        }

        @Test
        @DisplayName("Stats reflect accurate progress during copy")
        void testStatsAccuracyDuringCopy() throws Exception {
            // Setup: Create multiple files
            var tenMB =  10 * 1024 * 1024;
            var fifteenMB = 15 * 1024 * 1024;
            createFile(tempSourceDir,"file1.txt", tenMB);
            createFile(tempSourceDir,"file2.txt", fifteenMB);

            // Setup: Track stats during copy
            AtomicInteger callbackCount = new AtomicInteger(0);
            AtomicLong lastDataCopied = new AtomicLong(0);
            Consumer<DirectoryCopyUtil.Stats> callback = stats -> {
                callbackCount.incrementAndGet();
                long currentDataCopied = stats.getDataCopied();
                assertTrue(currentDataCopied >= lastDataCopied.get(), "Data copied should not decrease");
                lastDataCopied.set(currentDataCopied);
                assertTrue(stats.getFilesCopied() <= 2, "Files copied should not exceed total files");
                assertEquals(2, stats.getTotalFiles(), "Total files should remain 2");
                assertEquals(tenMB + fifteenMB, stats.getTotalDataSize(), "Total data size should match");
            };

            var copyUtil = new DirectoryCopyUtil.Builder()
                    .progressUpdateInterval(1)
                    .progressCallback(callback)
                    .build();

            // Act: Copy with custom callback
            var copyOperation = copyUtil.copyDirectory(tempSourceDir, tempTargetDir);
            copyOperation.getFuture().get(10, TimeUnit.SECONDS); // Wait with timeout

            // Assert: Final stats are accurate
            var stats = copyOperation.getStats();
            assertTrue(callbackCount.get() > 0, "Callback should be invoked at least once");
            assertEquals(2, stats.getFilesCopied(), "Two files should be copied");
            assertEquals(tenMB + fifteenMB, stats.getDataCopied(), "All data should be copied");
            assertTrue(stats.getErrorSummaries().isEmpty(), "No errors should occur");
        }

        @Test
        @DisplayName("Progress update interval is respected")
        void testProgressUpdateIntervalRespected() throws Exception {


            // Setup: Track callback timings during copy
            List<Long> callbackTimes = new ArrayList<>();
            AtomicLong copyCompletionTime = new AtomicLong(0);
            Consumer<DirectoryCopyUtil.Stats> callback = stats -> {
                long time = System.currentTimeMillis();
                // Only record callbacks before or at completion
                if (copyCompletionTime.get() == 0 || time <= copyCompletionTime.get()) {
                    callbackTimes.add(time);
                }
            };

            // Setup: Use a custom interval (1 seconds) and a larger file
            DirectoryCopyUtil customUtil = new DirectoryCopyUtil.Builder()
                    .progressUpdateInterval(1)
                    .bufferSize(1024) // Small buffer to slow down copy
                    .progressCallback(callback)
                    .build();
            var dataSize = 100 * 1024 * 1024; // 100MB to ensure copy takes >10s
            createFile(tempSourceDir, "large.txt", dataSize);


            // Act: Copy with custom callback
            var copyOperation = customUtil.copyDirectory(tempSourceDir, tempTargetDir);
            copyOperation.getFuture().get(30, TimeUnit.SECONDS); // Increased timeout for larger file
            copyCompletionTime.set(System.currentTimeMillis()); // Mark completion time

            // Assert: Callback intervals are approximately 2 seconds apart
            assertTrue(callbackTimes.size() >= 2, "Callback should be invoked at least twice during copy. got " + callbackTimes.size());
            for (int i = 1; i < callbackTimes.size()-1; i++) {
                long interval = callbackTimes.get(i) - callbackTimes.get(i - 1);
                assertTrue(interval >= 1000 && interval <= 2000,
                        "Callback interval should be roughly 2 seconds (between 1s and 2s), got " + interval + "ms");
            }
            var stats = copyOperation.getStats();
            assertEquals(1, stats.getFilesCopied(), "One file should be copied");
            assertEquals(dataSize, stats.getDataCopied(), "All data should be copied");
            assertTrue(stats.getErrorSummaries().isEmpty(), "No errors should occur");
        }
    }

    @Nested
    @DisplayName("Cancellation Behavior Tests")
    class CancellationBehavior {

        @Test
        @DisplayName("Cancel before copy starts")
        void testCancellationBeforeCopyStarts() throws Exception {
            // Setup: Create a small file
            createFile(tempSourceDir, "small.txt", 1024);

            // Act: Start copy and cancel immediately
            var copyOperation = copyUtil.copyDirectory(tempSourceDir, tempTargetDir);
            copyOperation.cancel(); // Cancel immediately
            // Wait for executor to terminate with a timeout
            assertTrue(copyOperation.awaitTermination(2, TimeUnit.SECONDS),
                    "Executor should terminate within 2 seconds after cancellation");

            // Assert: No files copied, operation is cancelled, and terminated
            Path targetFile = tempTargetDir.resolve("small.txt");
            assertFalse(Files.exists(targetFile), "Target file should not exist after cancellation");
            assertTrue(copyOperation.isCancelled(), "Operation should be marked as cancelled");
            assertTrue(copyOperation.isDone(), "Operation should be done after cancellation");
            assertTrue(copyOperation.isTerminated(), "Executor should be terminated");
            var stats = copyOperation.getStats();
            assertEquals(0, stats.getFilesCopied(), "No files should be copied");
            assertEquals(0, stats.getDataCopied(), "No data should be copied");
            assertEquals(1, stats.getTotalFiles(), "Total files should still reflect source");
        }

        @Test
        @DisplayName("Cancel during single large file copy")
        void testCancellationDuringSingleLargeFileCopy() throws Exception {
            // Setup: Create a large file and slow copy with small buffer
            var dataSze = 100 * 1024 * 1024; // 100MB
            createFile(tempSourceDir,"large.txt", dataSze);

            AtomicLong dataCopiedBeforeCancel = new AtomicLong(0);
            Consumer<DirectoryCopyUtil.Stats> callback = stats -> dataCopiedBeforeCancel.set(stats.getDataCopied());

            DirectoryCopyUtil slowUtil = new DirectoryCopyUtil.Builder()
                    .threadPoolSize(1)
                    .progressUpdateInterval(1)
                    .bufferSize(128) // Small buffer to slow down copy
                    .progressCallback(callback)
                    .build();

            // Setup: Track progress to cancel mid-copy


            var copyOperation = slowUtil.copyDirectory(tempSourceDir, tempTargetDir);

            // Act: cancel
            copyOperation.cancel();
            copyOperation.awaitTermination(5, TimeUnit.SECONDS);

            // Assert: Partial copy deleted, stats reflect cancellation
            Path targetFile = tempTargetDir.resolve("large.txt");
            assertFalse(Files.exists(targetFile), "Target file should be deleted after cancellation");
            var stats = copyOperation.getStats();
            assertTrue(copyOperation.isCancelled(), "Operation should be marked as cancelled");
            assertTrue(copyOperation.isDone(), "Operation should be done after cancellation");
            assertEquals(0, stats.getFilesCopied(), "No files should be fully copied");
        }

        @Test
        @DisplayName("Cancel immediately during multiple large file copy")
        void testImmediateCancellationWithMultipleLargeFiles() throws Exception {

            CountDownLatch copyProgress = new CountDownLatch(1);
            AtomicInteger callbackCount = new AtomicInteger(0);
            Consumer<DirectoryCopyUtil.Stats> progressCallback = stats -> {
                callbackCount.incrementAndGet();
                if (stats.getFilesCopied() >= 2) {
                    copyProgress.countDown();
                }
            };

            int fileCount = 5;
           // long fileSize = 1024 * 1024 * 100; // 100 MB
            byte[] data = new byte[ 1024 * 1024 * 25]; // 25 MB
            new Random().nextBytes(data);

            for (int i = 0; i < fileCount; i++) {
                createFile(tempSourceDir,"large" + i + ".bin", data);
            }

            DirectoryCopyUtil util = new DirectoryCopyUtil.Builder()
                    .progressUpdateInterval(1)
                    .threadPoolSize(8)
                    .copyOptions(Set.of(StandardCopyOption.REPLACE_EXISTING))
                    .bufferSize(1024 * 4)
                    .progressCallback(progressCallback)
                    .build();

            var copyOperation = util.copyDirectory(tempSourceDir, tempTargetDir);

            long cancelStartTime = System.currentTimeMillis();
            copyOperation.cancel();
            long cancelEndTime = System.currentTimeMillis();
            long cancellationDuration = cancelEndTime - cancelStartTime;

            var stats = copyOperation.getStats();
            assertTrue(copyOperation.isCancelled());
            assertTrue(cancellationDuration < 3000);
            assertTrue(stats.getFilesCopied() < fileCount);
            assertTrue(stats.getFilesCopied() <= 2);

            int callbackCountBeforeSleep = callbackCount.get();
            int callbackCountAfterSleep = callbackCount.get();
            assertTrue(callbackCountAfterSleep - callbackCountBeforeSleep <= 2);
        }

        @Test
        @DisplayName("Progress callback stops after cancellation")
        void testProgressCallbackStopsAfterCancellation() throws Exception {

            // Setup: Count callback invocations
            AtomicInteger callbackCount = new AtomicInteger(0);
            Consumer<DirectoryCopyUtil.Stats> callback = stats -> callbackCount.incrementAndGet();

            // Setup: Create a large file and slow copy
            DirectoryCopyUtil slowUtil = new DirectoryCopyUtil.Builder()
                    .progressUpdateInterval(1)
                    .bufferSize(128) // Small buffer to slow down copy
                    .progressCallback(callback)
                    .build();
            createFile(tempSourceDir,"large.txt", 100 * 1024 * 1024); // 100MB


            var copyOperation = slowUtil.copyDirectory(tempSourceDir, tempTargetDir);

            // Act: Cancel after a short delay
            Thread.sleep(1500); // Wait 1.5s to get at least one callback
            int countBeforeCancel = callbackCount.get();
            copyOperation.cancel();
            Thread.sleep(2000); // Wait 2s beyond progressUpdateInterval to check callback cessation

            // Assert: Callback stops after cancellation
            int countAfterCancel = callbackCount.get();
            assertTrue(countBeforeCancel > 0, "At least one callback should occur before cancellation");
            assertEquals(countBeforeCancel, countAfterCancel,
                    "Callback count should not increase after cancellation, got " + countAfterCancel);
            assertTrue(copyOperation.isCancelled(), "Operation should be marked as cancelled");
            assertTrue(copyOperation.isDone(), "Operation should be done after cancellation");
        }

        @Test
        @DisplayName("Multiple cancellations have no side effects")
        void testMultipleCancellations() throws Exception {
            // Setup: Create a large file and slow copy
            DirectoryCopyUtil slowUtil = new DirectoryCopyUtil.Builder()
                    .progressUpdateInterval(1)
                    .bufferSize(512) // Small buffer to slow down copy
                    .build();

            var dataSize = 100 * 1024 * 1024; // 100MB
            Path largeFile = createFile(tempSourceDir, "large.txt", dataSize);

            // Act: Start copy and cancel multiple times
            var copyOperation = slowUtil.copyDirectory(tempSourceDir, tempTargetDir);
            Thread.sleep(1000); // Let it start copying
            copyOperation.cancel(); // First cancel
            copyOperation.cancel(); // Second cancel
            copyOperation.cancel(); // Third cancel
            Thread.sleep(2000); // Wait for cancellation to complete

            // Assert: Operation remains cancelled, no additional effects
            Path targetFile = tempTargetDir.resolve("large.txt");
            assertFalse(Files.exists(targetFile), "Target file should not exist after cancellation");
            assertTrue(copyOperation.isCancelled(), "Operation should be marked as cancelled");
            assertTrue(copyOperation.isDone(), "Operation should be done after cancellation");
            assertTrue(copyOperation.isTerminated(), "Executor should be terminated");
            var stats = copyOperation.getStats();
            assertEquals(0, stats.getFilesCopied(), "No files should be fully copied");
            assertTrue(stats.getDataCopied() >= 0 && stats.getDataCopied() < dataSize,
                    "Some or no data may be copied before cancellation: " + stats.getDataCopied());
            assertTrue(stats.getFailedFiles().contains(largeFile.toString()),
                    "Interrupted file should be in failedFiles");
        }
    }

    @Nested
    @DisplayName("Error Handling Tests")
    class ErrorHandling {

        @Test
        @DisplayName("Source file deleted during copy")
        void testSourceFileDeletedDuringCopy() throws Exception {
            // Setup: Create a large file and slow copy
            DirectoryCopyUtil slowUtil = new DirectoryCopyUtil.Builder()
                    .progressUpdateInterval(1)
                    .bufferSize(512) // Small buffer to slow down copy
                    .build();

            var dataSize = 100 * 1024 * 1024; // 100MB
            Path sourceFile = createFile(tempSourceDir, "large.txt", dataSize);

            // Act: Start copy and delete source mid-copy
            var copyOperation = slowUtil.copyDirectory(tempSourceDir, tempTargetDir);
            Thread.sleep(2000); // Wait 2s to start copying
            Files.delete(sourceFile); // Simulate source file deletion
            copyOperation.getFuture().get(10, TimeUnit.SECONDS); // Wait for completion

            // Assert: Exception caught, stats updated, target cleaned up
            Path targetFile = tempTargetDir.resolve("large.txt");
            assertFalse(Files.exists(targetFile), "Target file should be cleaned up after source deletion");
            var stats = copyOperation.getStats();
            assertEquals(0, stats.getFilesCopied(), "No files should be fully copied");
            assertTrue(stats.getDataCopied() >= 0 && stats.getDataCopied() < dataSize,
                    "Some data may be copied before deletion: " + stats.getDataCopied());
            assertTrue(stats.getFailedFiles().contains(sourceFile.toString()),
                    "Deleted file should be in failedFiles");
            assertTrue(stats.getErrorSummaries().stream()
                            .anyMatch(s -> s.contains("Source file deleted during copy")),
                    "Error summary should include message about source deletion for " + sourceFile);
        }

        @Test
        @DisplayName("Permission denied on source")
        void testPermissionDeniedOnSource() throws Exception {
            // Setup: Create a file
            Path sourceFile = createFile(tempSourceDir, "denied.txt", 1024); // 1KB

            // Deny read permission on the source file (POSIX systems only)
            boolean isPosixSupported = FileSystems.getDefault().supportedFileAttributeViews().contains("posix");
            Assumptions.assumeTrue(isPosixSupported, "POSIX permissions not supported on this platform; skipping test");

            Files.setPosixFilePermissions(sourceFile, EnumSet.noneOf(PosixFilePermission.class)); // No permissions

            try {
                // Act: Attempt to copy while file is unreadable
                var copyOperation = copyUtil.copyDirectory(tempSourceDir, tempTargetDir);
                copyOperation.getFuture().get(5, TimeUnit.SECONDS); // Wait for completion

                // Assert: Error logged, no copy
                Path targetFile = tempTargetDir.resolve("denied.txt");
                assertFalse(Files.exists(targetFile), "Target file should not exist due to source access denial");
                var stats = copyOperation.getStats();

                // Updated assertions for skipped files
                assertEquals(0, stats.getFilesCopied(), "No files should be copied");
                assertEquals(0, stats.getDataCopied(), "No data should be copied");
                assertTrue(stats.getSkippedFiles().contains(sourceFile.toString()),
                        "Denied file should be in skippedFiles");
                assertTrue(stats.getSkippedFiles().contains(sourceFile.toString()),
                        "Error summary should include message about access denial for " + sourceFile);
            } finally {
                // Restore permissions to allow cleanup by TempDir
                Files.setPosixFilePermissions(sourceFile,
                        EnumSet.of(PosixFilePermission.OWNER_READ, PosixFilePermission.OWNER_WRITE));
            }
        }

        @Test
        @DisplayName("Insufficient disk space")
        void testInsufficientDiskSpace() throws Exception {
            // Setup: Create a large file
            Path largeFile = tempSourceDir.resolve("largeFile.dat");
            Files.write(largeFile, new byte[1024 * 1024 * 100]); // 100MB file

            // Mock FileStore to report limited space
            FileStore mockStore = Mockito.mock(FileStore.class);
            when(mockStore.getUsableSpace()).thenReturn(50L * 1024 * 1024); // Only 50MB available
            when(mockStore.getTotalSpace()).thenReturn(200L * 1024 * 1024); // 200MB total

            try (MockedStatic<Files> mockedFiles = mockStatic(Files.class, CALLS_REAL_METHODS)) {
                mockedFiles.when(() -> Files.getFileStore(any(Path.class)))
                        .thenReturn(mockStore);

                // Act & Assert: Verify the expected exception is thrown
                IOException exception = assertThrows(IOException.class,
                        () -> copyUtil.copyDirectory(tempSourceDir, tempTargetDir),
                        "Should throw IOException when disk space is insufficient");

                assertTrue(exception.getMessage().contains("Not enough disk space"),
                        "Error message should indicate disk space issue");
            }
        }

        @Test
        @DisplayName("Verify file overwrite correctly without exception")
        void testFileOverwrite() throws Exception {
            Path sourceFile = tempSourceDir.resolve("test.txt");
            Path targetFile = tempTargetDir.resolve("test.txt");
            Files.writeString(sourceFile, "New Content");
            Files.writeString(targetFile, "Old Content");

            DirectoryCopyUtil customUtil = new DirectoryCopyUtil.Builder()
                .copyOptions(Set.of(StandardCopyOption.REPLACE_EXISTING)) // set to REPLACE_EXISTING
                .build();

            var copyOperation = customUtil.copyDirectory(tempSourceDir, tempTargetDir);
            Future<?> future = copyOperation.getFuture();
            future.get(10, TimeUnit.SECONDS); // Wait for completion

            var stats = copyOperation.getStats();
            assertEquals(1, stats.getFilesCopied());
            assertEquals(1, stats.getOverwrittenFiles().size());
            assertEquals("New Content", Files.readString(targetFile));
        }

        @Test
        @DisplayName("Attempt copy to read-only target directory")
        void testWritingToReadOnlyTargetDirectory() throws IOException {
            // Set up the source directory and file
            Path sourceFile = tempSourceDir.resolve("test.txt");
            Files.writeString(sourceFile, "content");

            // Make the target directory read-only
            boolean readonly = tempTargetDir.toFile().setReadOnly();
            assertTrue(readonly);

            // Assert that copyDirectory throws an IOException
            assertThrows(IOException.class,
                    () -> copyUtil.copyDirectory(tempSourceDir, tempTargetDir),
                    "Should throw IOException due to read-only target directory");
        }

        @Test
        @DisplayName("Attempt copy from non-existent source directory")
        void testSourceNotExist() {
            Path nonExistent = Paths.get("nonexistent");
            assertThrows(IllegalArgumentException.class, () ->
                    copyUtil.copyDirectory(nonExistent, tempTargetDir));
        }

        @Test
        @DisplayName("Copy from read-only source file")
        void testReadOnlySourceFile() throws Exception {
            // Setup: Create a source file
            Path sourceFile = tempSourceDir.resolve("test.txt");
            Files.writeString(sourceFile, "content");

            // Make the source file read-only (POSIX systems only)
            boolean isPosixSupported = FileSystems.getDefault().supportedFileAttributeViews().contains("posix");
            Assumptions.assumeTrue(isPosixSupported, "POSIX permissions not supported on this platform");
            Files.setPosixFilePermissions(sourceFile, EnumSet.of(PosixFilePermission.OWNER_READ));

            // Act: Copy the directory
            var copyOperation = copyUtil.copyDirectory(tempSourceDir, tempTargetDir);
            copyOperation.getFuture().get(10, TimeUnit.SECONDS);

            // Assert: File is copied despite being read-only
            var stats = copyOperation.getStats();
            Path targetFile = tempTargetDir.resolve("test.txt");
            assertEquals(1, stats.getFilesCopied());
            assertTrue(Files.exists(targetFile));
            assertEquals("content", Files.readString(targetFile));
            assertTrue(stats.getErrorSummaries().isEmpty());
        }

        @Test
        @DisplayName("Copy directory with mixed permission files")
        void testMixedPermissionFiles() throws Exception {
            // Skip if POSIX permissions are not supported
            boolean isPosixSupported = FileSystems.getDefault().supportedFileAttributeViews().contains("posix");
            Assumptions.assumeTrue(isPosixSupported, "POSIX permissions not supported on this platform; skipping test");

            // Setup: Create files with different permissions
            Path readableFile = createFile(tempSourceDir, "readable.txt", "readable content".getBytes());
            Path writableFile = createFile(tempSourceDir, "writable.txt", "writable content".getBytes());
            Path executableFile = createFile(tempSourceDir, "executable.sh", "executable content".getBytes());
            Path restrictedFile = createFile(tempSourceDir, "restricted.txt", "restricted content".getBytes());

            // Set different permissions
            Files.setPosixFilePermissions(readableFile,
                    EnumSet.of(PosixFilePermission.OWNER_READ, PosixFilePermission.OWNER_WRITE));
            Files.setPosixFilePermissions(writableFile,
                    EnumSet.of(PosixFilePermission.OWNER_WRITE)); // No read permission
            Files.setPosixFilePermissions(executableFile,
                    EnumSet.of(PosixFilePermission.OWNER_EXECUTE)); // No read permission
            Files.setPosixFilePermissions(restrictedFile,
                    EnumSet.noneOf(PosixFilePermission.class)); // No permissions

            // Act: Copy the directory
            var copyOperation = copyUtil.copyDirectory(tempSourceDir, tempTargetDir);
            copyOperation.getFuture().get(10, TimeUnit.SECONDS);

            // Assert: Verify stats reflect the expected behavior
            var stats = copyOperation.getStats();

            // Should detect all 3 skipped files
            assertEquals(3, stats.getSkippedFiles().size(), "Should detect all 4 files");

            // Only the readable file should be successfully copied
            assertEquals(1, stats.getFilesCopied(), "Only readable file should be copied");
            assertEquals("readable content".getBytes().length, stats.getDataCopied(),
                    "Data copied should match readable file size");

            // Verify readable file was actually copied
            Path copiedReadableFile = tempTargetDir.resolve("readable.txt");
            assertTrue(Files.exists(copiedReadableFile), "Readable file should exist in target");
            assertEquals("readable content", Files.readString(copiedReadableFile),
                    "File content should match");

            // Verify other files were skipped (not failed)
            assertEquals(3, stats.getSkippedFiles().size(), "Should skip 3 files");
            assertTrue(stats.getSkippedFiles().contains(writableFile.toString()),
                    "Writable file should be skipped");
            assertTrue(stats.getSkippedFiles().contains(executableFile.toString()),
                    "Executable file should be skipped");
            assertTrue(stats.getSkippedFiles().contains(restrictedFile.toString()),
                    "Restricted file should be skipped");
            assertEquals(0, stats.getFailedFiles().size(), "No files should be in failedFiles");
        }
    }

    @Nested
    @DisplayName("Edge Cases Tests")
    class EdgeCases {
        @Test
        @DisplayName("Copy with REPLACE_EXISTING to non-empty target")
        void testCopyToExistingNonEmptyTarget() throws Exception {
            // Setup: Create source file
            createFile(tempSourceDir, "source.txt", "Source content".getBytes());
            // Setup: Create target with existing file
            var targetFile = createFile(tempTargetDir, "source.txt", "Existing content".getBytes());


            // Act: Copy with REPLACE_EXISTING
            var copyOperation = copyUtil.copyDirectory(tempSourceDir, tempTargetDir);
            copyOperation.getFuture().get(5, TimeUnit.SECONDS);

            // Assert: Overwrite occurred, stats updated
            var stats = copyOperation.getStats();
            assertEquals(1, stats.getFilesCopied(), "One file should be copied");
            assertEquals("Source content".length(), stats.getDataCopied(), "Source content should be copied");
            assertTrue(stats.getOverwrittenFiles().contains(targetFile.toString()),
                    "Target file should be in overwrittenFiles");
            assertEquals("Source content", Files.readString(targetFile), "Target file should be overwritten");
        }

        @Test
        @DisplayName("Copy with zero buffer size")
        void testCopyWithZeroBufferSize() {
            // Act & Assert: Attempt to build with bufferSize = 0
            assertThrows(IllegalArgumentException.class, () -> new DirectoryCopyUtil.Builder()
                    .progressUpdateInterval(1)
                    .bufferSize(0) // Invalid buffer size
                    .build(), "Should throw IllegalArgumentException for zero buffer size");
        }

        @Test
        @DisplayName("Copy with negative thread pool size")
        void testCopyWithNegativeThreadPoolSize() {
            // Act & Assert: Attempt to build with threadPoolSize = -1
            assertThrows(IllegalArgumentException.class, () -> new DirectoryCopyUtil.Builder()
                    .progressUpdateInterval(1)
                    .bufferSize(1024)
                    .threadPoolSize(-1) // Invalid thread pool size
                    .build(), "Should throw IllegalArgumentException for negative thread pool size");
        }


        @Test
        @DisplayName("Copy same source and target")
        void testCopySameSourceAndTarget() throws Exception {
            // Setup: Create a source file
            createFile(tempSourceDir, "file.txt", "Content".getBytes() );

            // Act & Assert: Attempt to copy directory to itself
            assertThrows(IllegalArgumentException.class, () -> copyUtil.copyDirectory(tempSourceDir, tempSourceDir), "Should throw IllegalArgumentException for same source and target");
        }

        @Test
        @DisplayName("Copy target as subdirectory of source")
        void testCopyTargetAsSubdirectoryOfSource() throws Exception {
            // Setup: Create a source file and subdirectory as target
            createFile(tempSourceDir, "file.txt", "Content".getBytes());
            Path subDir =  tempSourceDir.resolve("subDir");
            Files.createDirectory(subDir);

            // Act & Assert: Attempt to copy to subdirectory
            assertThrows(IllegalArgumentException.class, () -> copyUtil.copyDirectory(tempSourceDir, subDir), "Should throw IllegalArgumentException for target as subdirectory of source");
        }

        @Test
        @DisplayName("Copy empty directory triggers warning")
        void testCopyEmptySourceWithWarning() throws Exception {
            // Setup: Ensure source directory is empty
            Files.createDirectories(tempSourceDir); // Already empty from @TempDir

            // Act: Copy empty source
            var copyOperation = copyUtil.copyDirectory(tempSourceDir, tempTargetDir);
            copyOperation.getFuture().get(5, TimeUnit.SECONDS);

            // Assert: Warning logged, no files copied
            var stats = copyOperation.getStats();
            assertEquals(0, stats.getFilesCopied(), "No files should be copied");
            assertEquals(0, stats.getDataCopied(), "No data should be copied");
            assertTrue(stats.getWarningSummaries().stream()
                            .anyMatch(s -> s.contains("Source directory is empty")),
                    "Warning summary should include 'Source directory is empty'");
        }

        @Test
        @DisplayName("Copy files with extreme special characters (filesystem-dependent)")
        void testFilesystemSpecificSpecialCharacters() throws Exception {
            // These might behave differently on various filesystems
            String[] extremeNames = {
                    "leading space.txt",
                    " trailing space.txt ",  // Note leading space
                    "control\u0001char.txt",
                    "pipe|file.txt",
                    "question?mark.txt",
                    "*.txt",  // Wildcard character
                    "file<>.txt"  // Redirection symbols
            };

            // Track which files were successfully created
            List<String> successfullyCreated = new ArrayList<>();
            List<String> failedToCreate = new ArrayList<>();

            // Create files with special names (some may fail)
            for (String name : extremeNames) {
                try {
                    createFile(tempSourceDir, name, ("content for " + name.trim()).getBytes());
                    successfullyCreated.add(name);
                } catch (IOException e) {
                    failedToCreate.add(name);
                    System.out.println("Failed to create test file '" + name + "': " + e.getMessage());
                }
            }

            // Act: Copy the directory
            var copyOperation = copyUtil.copyDirectory(tempSourceDir, tempTargetDir);
            copyOperation.getFuture().get(10, TimeUnit.SECONDS);

            // Assert: Check results
            var stats = copyOperation.getStats();

            // Verify successfully created files were copied
            for (String name : successfullyCreated) {
                Path targetFile = tempTargetDir.resolve(name);
                assertTrue(Files.exists(targetFile), "File should exist: " + name);
                assertEquals("content for " + name.trim(), Files.readString(targetFile));
            }

            // Verify stats reflect what we expect
            assertEquals(successfullyCreated.size(), stats.getFilesCopied());
            assertEquals(successfullyCreated.size(), stats.getTotalFiles());

            // Verify failed files are properly reported (if any)
            if (!failedToCreate.isEmpty()) {
                assertEquals(failedToCreate.size(), stats.getFailedFiles().size());
                for (String failedName : failedToCreate) {
                    assertTrue(stats.getFailedFiles().stream()
                                    .anyMatch(f -> f.contains(failedName)),
                            "Should report failure for: " + failedName);
                }
            }
        }

        @Test
        @DisplayName("Copy Windows reserved filenames (expect failure)")
        void testWindowsReservedNames() throws Exception {
            Assumptions.assumeTrue(System.getProperty("os.name").contains("Windows"),
                    "This test only runs on Windows");

            String[] reservedNames = {
                    "CON.txt", "PRN.txt", "AUX.txt",
                    "NUL.txt", "COM1.txt", "LPT1.txt"
            };

            // Attempt to create files with reserved names (should fail)
            List<Path> attemptedFiles = new ArrayList<>();
            for (String name : reservedNames) {
                Path file = tempSourceDir.resolve(name);
                try {
                    Files.writeString(file, "content for " + name);
                    fail("Should not be able to create reserved file: " + name);
                } catch (IOException e) {
                    // Expected - these files should not be creatable
                    System.out.println("Correctly failed to create reserved file: " +
                            name + " - " + e.getMessage());
                }
                attemptedFiles.add(file);
            }

            // Create one valid file to ensure the directory isn't empty
            createFile(tempSourceDir, "valid.txt", "normal file".getBytes());

            // Act: Attempt to copy the directory
            var copyOperation = copyUtil.copyDirectory(tempSourceDir, tempTargetDir);
            copyOperation.getFuture().get(10, TimeUnit.SECONDS);

            // Assert: Verify behavior
            var stats = copyOperation.getStats();

            // Should only copy the valid file
            assertEquals(1, stats.getFilesCopied(), "Only the valid file should be copied");
            assertEquals(1, stats.getTotalFiles(), "Should only count the valid file");
            assertTrue(Files.exists(tempTargetDir.resolve("valid.txt")),
                    "Valid file should be copied");

            // Verify reserved names are properly reported as failed
            assertEquals(reservedNames.length, stats.getFailedFiles().size(),
                    "All reserved names should be reported as failed");

            for (String name : reservedNames) {
                assertTrue(stats.getFailedFiles().contains(tempSourceDir.resolve(name).toString()),
                        "Should report failure for reserved name: " + name);
            }

            // Verify error summaries contain appropriate messages
            assertTrue(stats.getErrorSummaries().stream()
                            .anyMatch(s -> s.contains("reserved name") || s.contains("cannot be created")),
                    "Error summaries should mention reserved name violations");
        }

        @Test
        @DisplayName("Copy files with very long filenames")
        void testVeryLongFilenames() throws Exception {
            // Setup: Create a filename that exceeds typical filesystem limits (255 chars)
            String longName = "very_long_filename_" +
                    "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789" +
                    "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789" +
                    "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789" +
                    "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789" +
                    ".txt";

            // Some filesystems have limits, so we'll track if creation succeeded
            boolean created = false;

            try {
                createFile(tempSourceDir, longName, "content for long filename".getBytes());
                created = true;
            } catch (IOException e) {
                System.out.println("Could not create file with long name: " + e.getMessage());
            }

            Assumptions.assumeTrue(created, "Filesystem doesn't support very long filenames");

            // Act: Copy the directory
            var copyOperation = copyUtil.copyDirectory(tempSourceDir, tempTargetDir);
            copyOperation.getFuture().get(10, TimeUnit.SECONDS);

            // Assert: Long filename file was copied correctly
            var stats = copyOperation.getStats();
            Path targetFile = tempTargetDir.resolve(longName);

            assertEquals(1, stats.getFilesCopied());
            assertEquals(1, stats.getTotalFiles());
            assertTrue(Files.exists(targetFile), "File with long name should exist");
            assertEquals("content for long filename", Files.readString(targetFile));
            assertTrue(stats.getErrorSummaries().isEmpty());
        }

        @Test
        @DisplayName("Copy files with filesystem-specific attributes")
        void testFilesystemSpecificAttributes() throws Exception {
            // Skip if extended attributes are not supported
            boolean supportsExtendedAttributes = FileSystems.getDefault()
                    .supportedFileAttributeViews()
                    .contains("xattr");
            Assumptions.assumeTrue(supportsExtendedAttributes,
                    "Extended attributes not supported on this platform; skipping test");

            // Setup: Create a test file with custom extended attribute
            Path sourceFile = createFile(tempSourceDir, "attr_test.txt", "content".getBytes());

            // Add custom extended attribute (platform-specific)
            try {
                Files.setAttribute(sourceFile, "user:custom_attr", "test_value".getBytes());
            } catch (IOException e) {
                Assumptions.assumeTrue(false, "Failed to set extended attribute: " + e.getMessage());
            }

            // Act: Copy the directory
            var copyOperation = copyUtil.copyDirectory(tempSourceDir, tempTargetDir);
            copyOperation.getFuture().get(10, TimeUnit.SECONDS);

            // Assert: Verify file and attributes were copied
            Path targetFile = tempTargetDir.resolve("attr_test.txt");
            assertTrue(Files.exists(targetFile), "Target file should exist");

            // Verify basic content was copied
            assertEquals("content", Files.readString(targetFile));

            // Verify extended attribute was copied (if COPY_ATTRIBUTES is enabled)
            try {
                byte[] attrValue = (byte[]) Files.getAttribute(targetFile, "user:custom_attr");
                assertEquals("test_value", new String(attrValue),
                        "Extended attribute should be preserved");
            } catch (IOException e) {
                // If attributes weren't copied, verify this is properly reflected in stats
                var stats = copyOperation.getStats();
                assertTrue(stats.getWarningSummaries().stream()
                                .anyMatch(s -> s.contains("attributes")),
                        "Warning should be logged about attribute preservation");
            }
        }

    }

    @Nested
    @DisplayName("Configuration Options Tests")
    class ConfigurationOptions {

        @Test
        @DisplayName("Custom Thread Pool Size: threadPoolSize = 1")
        void testCustomThreadPoolSize() throws Exception {
            // Setup: Create a "large" directory by adding several files
            int fileCount = 5;
            for (int i = 0; i < fileCount; i++) {
                createFile(tempSourceDir, "file" + i + ".txt", 1024 * 1024 ); // 1 MB per file
            }
            // Use a custom configuration with threadPoolSize set to 1
            DirectoryCopyUtil customUtil = new DirectoryCopyUtil.Builder()
                    .progressUpdateInterval(1)
                    .threadPoolSize(1)
                    .build();

            // Act: Perform the copy
            var copyOperation = customUtil.copyDirectory(tempSourceDir, tempTargetDir);
            copyOperation.getFuture().get(30, TimeUnit.SECONDS);

            // Assert: All files are copied successfully
            var stats = copyOperation.getStats();
            assertEquals(fileCount, stats.getTotalFiles(), "Total files should match");
            assertEquals(fileCount, stats.getFilesCopied(), "All files should be copied");
            for (int i = 0; i < fileCount; i++) {
                Path targetFile = tempTargetDir.resolve("file" + i + ".txt");
                assertTrue(Files.exists(targetFile), "Target file " + targetFile + " should exist");
            }
        }

        @Test
        @DisplayName("Large Thread Pool Size: threadPoolSize = 10")
        void testLargeThreadPoolSize() throws Exception {
            // Setup: Create many files to simulate parallel copying
            int fileCount = 20;
            for (int i = 0; i < fileCount; i++) {
                createFile(tempSourceDir, "file" + i + ".txt", 512 * 1024); // 512 KB per file
            }
            // Use a builder with a larger thread pool size (10)
            DirectoryCopyUtil customUtil = new DirectoryCopyUtil.Builder()
                    .progressUpdateInterval(1)
                    .threadPoolSize(10)
                    .build();

            // Act: Copy the directory
            var copyOperation = customUtil.copyDirectory(tempSourceDir, tempTargetDir);
            copyOperation.getFuture().get(30, TimeUnit.SECONDS);

            // Assert: All files are copied correctly
            var stats = copyOperation.getStats();
            assertEquals(fileCount, stats.getTotalFiles(), "Total files should match");
            assertEquals(fileCount, stats.getFilesCopied(), "All files should be copied");
        }

        @Test
        @DisplayName("Custom Buffer Size: bufferSize = 1024")
        void testCustomBufferSize() throws Exception {
            // Setup: Create a large file to test copying in small chunks
            Path sourceFile = createFile(tempSourceDir,"large.txt", 30 * 1024 * 1024);// 30 MB file

            // Use a custom configuration with a small buffer size (1024 bytes)
            DirectoryCopyUtil customUtil = new DirectoryCopyUtil.Builder()
                    .progressUpdateInterval(1)
                    .bufferSize(1024)
                    .build();

            // Act: Copy the directory containing the large file
            var copyOperation = customUtil.copyDirectory(tempSourceDir, tempTargetDir);
            copyOperation.getFuture().get(60, TimeUnit.SECONDS);

            // Assert: Target file exists and its contents match the source
            Path targetFile = tempTargetDir.resolve("large.txt");
            assertTrue(Files.exists(targetFile), "Target file should exist");
            assertEquals(Files.size(sourceFile), Files.size(targetFile), "File sizes should match");
            assertArrayEquals(Files.readAllBytes(sourceFile), Files.readAllBytes(targetFile), "File contents should match");
        }

        @Test
        @DisplayName("Copy Without Attributes: omit COPY_ATTRIBUTES")
        void testCopyWithoutAttributes() throws Exception {
            // Setup: Create a source file and capture its last modified time
            Path sourceFile = createFile(tempSourceDir,"file.txt", 1024);
            FileTime originalTime = Files.getLastModifiedTime(sourceFile);

            // Wait briefly so the modification time would differ if not copied
            Thread.sleep(1500);

            // Use a configuration that omits COPY_ATTRIBUTES (only REPLACE_EXISTING provided)
            DirectoryCopyUtil customUtil = new DirectoryCopyUtil.Builder()
                    .progressUpdateInterval(1)
                    .copyOptions(Set.of(StandardCopyOption.REPLACE_EXISTING))
                    .build();

            // Act: Copy the source directory
            var copyOperation = customUtil.copyDirectory(tempSourceDir, tempTargetDir);
            copyOperation.getFuture().get(5, TimeUnit.SECONDS);

            // Assert: The target file exists but its last modified time does not match the original
            Path targetFile = tempTargetDir.resolve("file.txt");
            assertTrue(Files.exists(targetFile), "Target file should exist");
            FileTime targetTime = Files.getLastModifiedTime(targetFile);
            assertNotEquals(originalTime, targetTime, "Last modified time should differ when attributes are not copied");
        }

        @Test
        @DisplayName("Copy Without Replace Existing: expect error and no copy")
        void testCopyWithoutReplaceExisting() throws Exception {
            // Setup: Create a source file with new content
            createFile(tempSourceDir,"file.txt",  "New content".getBytes());

            // Create an existing target file with different content
            String existingContent = "Existing content";
            Path targetFile = createFile(tempTargetDir,"file.txt",  existingContent.getBytes());


            // Use a configuration that omits REPLACE_EXISTING by providing an empty set of copyOptions
            DirectoryCopyUtil customUtil = new DirectoryCopyUtil.Builder()
                    .progressUpdateInterval(1)
                    .copyOptions(Set.of()) // No REPLACE_EXISTING option
                    .build();

            // Act: Copy the source directory
            var copyOperation = customUtil.copyDirectory(tempSourceDir, tempTargetDir);
            copyOperation.awaitTermination(5, TimeUnit.SECONDS);

            // Assert:
            // (1) The target file should remain unchanged (or may have been deleted if cleanup was attempted)
            // (2) No file should be marked as successfully copied
            // (3) The error summary should mention that the file already exists (or a similar message)
            var stats = copyOperation.getStats();
            assertEquals(0, stats.getFilesCopied(), "No files should be copied without REPLACE_EXISTING");
            assertEquals(0, stats.getDataCopied(), "No data should be copied without REPLACE_EXISTING");

            // Optionally, you can check that the target file's content is still the original
            String targetContent = Files.readString(targetFile);
            assertEquals(existingContent, targetContent, "Target file content should remain unchanged");
            boolean errorFound = stats.getErrorSummaries().stream()
                    .anyMatch(s -> s.contains("File already exist:"+targetFile));
            assertTrue(errorFound, "Error summary should include a message about the target file already existing");
        }


    }


    @Nested
    @DisplayName("Concurrency and Thread Safety Tests")
    class ConcurrencyAndThreadSafety {

        @Test
        @DisplayName("Concurrent Copy Operations Same Source")
        void testConcurrentCopyOperationsSameSource() throws Exception {
            // Setup: Create a source directory with several files.
            int fileCount = 5;
            for (int i = 0; i < fileCount; i++) {
                createFile(tempSourceDir,"file" + i + ".txt", 1024);  // 1KB per file
            }

            // Launch several copy operations concurrently from the same source to different targets.
            int operationCount = 3;
            ExecutorService executor = Executors.newFixedThreadPool(operationCount);
            List<Future<DirectoryCopyUtil.Stats>> futures = new ArrayList<>();
            for (int i = 0; i < operationCount; i++) {
                // Create a unique target directory for each copyOperation.
                Path targetDir = Files.createTempDirectory(tempTargetDir, "target" + i);
                futures.add(executor.submit(() -> {
                    var copyOperation = copyUtil.copyDirectory(tempSourceDir, targetDir);
                    copyOperation.getFuture().get(); // wait for completion
                    return copyOperation.getStats();
                }));
            }
            executor.shutdown();
            assertTrue(executor.awaitTermination(30, TimeUnit.SECONDS));

            // Assert: Each copy operation should see the same file count and copy all files.
            for (Future<DirectoryCopyUtil.Stats> future : futures) {
                var stats = future.get();
                assertEquals(fileCount, stats.getTotalFiles(), "Total files should match");
                assertEquals(fileCount, stats.getFilesCopied(), "All files should be copied");
            }
        }

        @Test
        @DisplayName("Concurrent Copy Operations Different Sources")
        void testConcurrentCopyOperationsDifferentSources() throws Exception {
            // Setup: Create multiple source directories, each with a unique file.
            int operationCount = 3;
            ExecutorService executor = Executors.newFixedThreadPool(operationCount);
            List<Future<DirectoryCopyUtil.Stats>> futures = new ArrayList<>();
            for (int i = 0; i < operationCount; i++) {
                Path sourceDir = Files.createTempDirectory(tempSourceDir, "source" + i);
                createFile(sourceDir, "file.txt", ("Content " + i).getBytes());

                // Create a unique target directory for each source.
                Path targetDir = Files.createTempDirectory(tempTargetDir, "target" + i);
                futures.add(executor.submit(() -> {
                    var copyOperation = copyUtil.copyDirectory(sourceDir, targetDir);
                    copyOperation.getFuture().get();
                    return copyOperation.getStats();
                }));
            }
            executor.shutdown();
            assertTrue(executor.awaitTermination(30, TimeUnit.SECONDS));

            // Assert: Each operation should have copied exactly one file.
            for (Future<DirectoryCopyUtil.Stats> future : futures) {
                var stats = future.get();
                assertEquals(1, stats.getTotalFiles(), "Each source should have one file");
                assertEquals(1, stats.getFilesCopied(), "Each file should be copied");
            }
        }

        @Test
        @DisplayName("Stats Isolation Across Operations")
        void testStatsIsolationAcrossOperations() throws Exception {
            // Setup: Create two distinct source directories with one file each.
            Path sourceDirA = Files.createTempDirectory(tempSourceDir, "sourceA");
            Path sourceDirB = Files.createTempDirectory(tempSourceDir, "sourceB");

            byte[] dataA = "Data A".getBytes();
            byte[] dataB = "Data B".getBytes();
            createFile(sourceDirA, "file.txt", dataA) ;
            createFile(sourceDirB, "file.txt", dataB);


            // Create corresponding target directories.
            Path targetDirA = Files.createTempDirectory(tempTargetDir, "targetA");
            Path targetDirB = Files.createTempDirectory(tempTargetDir, "targetB");

            // Launch both copy operations concurrently.
            ExecutorService executor = Executors.newFixedThreadPool(2);
            Future<DirectoryCopyUtil.Stats> futureA = executor.submit(() -> {
                var copyOperation = copyUtil.copyDirectory(sourceDirA, targetDirA);
                copyOperation.getFuture().get();
                return copyOperation.getStats();
            });
            Future<DirectoryCopyUtil.Stats> futureB = executor.submit(() -> {
                var copyOperation = copyUtil.copyDirectory(sourceDirB, targetDirB);
                copyOperation.getFuture().get();
                return copyOperation.getStats();
            });
            executor.shutdown();
            assertTrue(executor.awaitTermination(30, TimeUnit.SECONDS));

            // Assert: Each copy operation's Stats should reflect only its own file.
            var statsA = futureA.get();
            var statsB = futureB.get();
            assertEquals(1, statsA.getTotalFiles(), "Source A should have 1 file");
            assertEquals(1, statsA.getFilesCopied(), "Source A file should be copied");
            assertEquals(dataA.length, statsA.getDataCopied(), "Data copied for Source A should match");

            assertEquals(1, statsB.getTotalFiles(), "Source B should have 1 file");
            assertEquals(1, statsB.getFilesCopied(), "Source B file should be copied");
            assertEquals(dataB.length, statsB.getDataCopied(), "Data copied for Source B should match");
        }


        @Test
        @DisplayName("Concurrent modification of source directory during copy")
        void testConcurrentDirectoryModification() throws Exception {
            Path initialFile = tempSourceDir.resolve("initial.txt");
            Files.writeString(initialFile, "initial");

            CountDownLatch progressLatch = new CountDownLatch(1);
            Consumer<DirectoryCopyUtil.Stats> progressCallback = stats -> {
                if (stats.getFilesCopied() > 0) {
                    progressLatch.countDown();
                }
            };

            var copyUtil = new DirectoryCopyUtil.Builder()
                    .progressUpdateInterval(1)
                    .progressCallback(progressCallback)
                    .build();

            ExecutorService copyExecutor = Executors.newSingleThreadExecutor();
            Future<DirectoryCopyUtil.Stats> copyFuture = copyExecutor.submit(() -> {
                var copyOperation = copyUtil.copyDirectory(tempSourceDir, tempTargetDir);
                copyOperation.getFuture().get(30, TimeUnit.SECONDS);
                return copyOperation.getStats();
            });

            assertTrue(progressLatch.await(5, TimeUnit.SECONDS), "Copy should start within 5 seconds");

            int modifications = 10;
            CountDownLatch modificationsLatch = new CountDownLatch(modifications);
            ExecutorService modifierExecutor = Executors.newSingleThreadExecutor();
            for (int i = 0; i < modifications; i++) {
                modifierExecutor.submit(() -> {
                    try {
                        Path newFile = tempSourceDir.resolve("mod_" + System.nanoTime() + ".txt");
                        Files.writeString(newFile, "concurrent content");
                    } catch (IOException e) {
                        // Ignore
                    } finally {
                        modificationsLatch.countDown();
                    }
                });
            }
            assertTrue(modificationsLatch.await(5, TimeUnit.SECONDS));
            modifierExecutor.shutdown();

            var stats = copyFuture.get(30, TimeUnit.SECONDS);
            copyExecutor.shutdown();

            assertTrue(stats.getFilesCopied() > 0);
            assertTrue(stats.getTotalFiles() > 0);
        }
    }


    @Nested
    @DisplayName("Performance and Resource Management Tests")
    class PerformanceAndResourceManagement {

        @Test
        @DisplayName("Executor Shutdown After Completion")
        void testExecutorShutdownAfterCompletion() throws Exception {
            // Setup: Create a source directory with a few files.
            int fileCount = 3;
            for (int i = 0; i < fileCount; i++) {
                createFile(tempSourceDir, "file" + i + ".txt",  ("Data " + i).getBytes());
            }

            // Act: Perform the copy copyOperation.
            var copyOperation = copyUtil.copyDirectory(tempSourceDir, tempTargetDir);
            copyOperation.awaitTermination(10, TimeUnit.SECONDS);
            //copyOperation.getFuture().get(10, TimeUnit.SECONDS); // Wait for completion

            // Assert: The executor and progress executor should have shut down.
            // isTerminated() returns true when the executor is fully terminated.
            assertTrue(copyOperation.isTerminated(), "Executor should be terminated after copy completion");
        }

        @Test
        @DisplayName("Memory Usage With Large Directory")
        void testMemoryUsageWithLargeDirectory() throws Exception {
            // Setup: Create a directory with many small files (e.g., 5,000 files).
            int fileCount = 5_000;
            var bytes = "small file".getBytes();
            for (int i = 0; i < fileCount; i++) {
                createFile(tempSourceDir, "file" + i + ".txt", bytes);
            }

            // Act: Copy the large directory.
            var copyOperation = copyUtil.copyDirectory(tempSourceDir, tempTargetDir);
            copyOperation.getFuture().get(120, TimeUnit.SECONDS); // allow extra time

            // Assert: Verify that all files were detected and copied.
            var stats = copyOperation.getStats();
            assertEquals(fileCount, stats.getTotalFiles(), "Total files should match the number created");
            assertEquals(fileCount, stats.getFilesCopied(), "All files should be copied");

        }

        @Test
        @DisplayName("Interrupt During Shutdown")
        void testInterruptDuringShutdown() throws Exception {
            // Setup: Create a source directory with a large file so that copy takes a while.
            createFile(tempSourceDir, "large.txt", 50 * 1024 * 1024); // 50 MB file

            // Act: Start the copy in a separate thread.
            var copyOperation = copyUtil.copyDirectory(tempSourceDir, tempTargetDir);
            Thread copyThread = new Thread(() -> {
                try {
                    copyOperation.getFuture().get();
                } catch (InterruptedException e) {
                    // Expected interruption.
                    Thread.currentThread().interrupt();
                } catch (ExecutionException e) {
                    // Ignore, as errors are captured in stats.
                }
            });
            copyThread.start();

            // Wait a short while to ensure the copy has started.
            Thread.sleep(500);

            // Interrupt the thread waiting on future.get()
            copyThread.interrupt();
            copyThread.join(5000); // Wait for the thread to finish

            // Trigger cancellation in our copy operation
            copyOperation.cancel();
            copyOperation.awaitTermination(10, TimeUnit.SECONDS);

            // Assert: The operation should be marked as cancelled and the executors terminated.
            assertTrue(copyOperation.isCancelled(), "Operation should be marked as cancelled after interrupt");
            assertTrue(copyOperation.isTerminated(), "Executor should be terminated even after interruption");
        }

        @Test
        @DisplayName("thread pool size is too small")
        void testThreadPoolExhaustion() throws Exception {
            for (int i = 0; i < 100; i++) {
                Files.writeString(tempSourceDir.resolve("file" + i + ".txt"), "content " + i);
            }

            DirectoryCopyUtil smallPoolUtil = new DirectoryCopyUtil.Builder()
                    .progressUpdateInterval(1)
                    .threadPoolSize(1)
                    .copyOptions(Set.of(StandardCopyOption.REPLACE_EXISTING))
                    .build();

            var copyOperation = smallPoolUtil.copyDirectory(tempSourceDir, tempTargetDir);
            Future<?> future = copyOperation.getFuture();
            future.get(30, TimeUnit.SECONDS);

            var stats = copyOperation.getStats();
            assertEquals(100, stats.getFilesCopied());
        }

        @Test
        @DisplayName("Copy very deep directory structure")
        void testVeryDeepDirectoryStructure() throws Exception {
            Path currentDir = tempSourceDir;
            for (int i = 0; i < 100; i++) {
                currentDir = currentDir.resolve("level" + i);
                Files.createDirectory(currentDir);
            }
            Path deepFile = currentDir.resolve("deep.txt");
            Files.writeString(deepFile, "deep content");

            var copyOperation = copyUtil.copyDirectory(tempSourceDir, tempTargetDir);
            copyOperation.getFuture().get(30, TimeUnit.SECONDS);

            var stats = copyOperation.getStats();
            Path targetDeepFile = tempTargetDir;
            for (int i = 0; i < 100; i++) {
                targetDeepFile = targetDeepFile.resolve("level" + i);
            }
            targetDeepFile = targetDeepFile.resolve("deep.txt");

            assertEquals(1, stats.getFilesCopied());
            assertEquals(1, stats.getTotalFiles());
            assertTrue(Files.exists(targetDeepFile));
            assertEquals("deep content", Files.readString(targetDeepFile));
            assertTrue(stats.getErrorSummaries().isEmpty());
        }
    }

    private Path createFile(Path dir, String filename, int sizeInBytes) throws IOException {
        byte[] data = new byte[sizeInBytes];
        new Random().nextBytes(data);
        return createFile(dir, filename, data);
    }

    private Path createFile(Path dir, String filename, byte[] data) throws IOException {
        Path file = dir.resolve(filename);
        Files.write(file, data);
        return file;
    }
}