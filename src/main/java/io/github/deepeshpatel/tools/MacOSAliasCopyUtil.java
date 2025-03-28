package io.github.deepeshpatel.tools;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.*;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility class for handling macOS Alias files during copy operations.
 * <p>
 * Provides functionality to:
 * <ul>
 *   <li>Detect macOS alias files</li>
 *   <li>Resolve the original target of an alias</li>
 *   <li>Copy aliases while maintaining their references</li>
 * </ul>
 *
 * <p>This class is specifically designed for macOS systems and will have limited functionality
 * on other operating systems.
 *
 * @see <a href="https://developer.apple.com/library/archive/documentation/FileManagement/Conceptual/understanding_alias_files/">
 *      Apple Documentation on Alias Files</a>
 */
public class MacOSAliasCopyUtil {
    private static final Logger logger = LoggerFactory.getLogger(MacOSAliasCopyUtil.class);

    /**
     * Determines if a given path represents a macOS Alias file.
     *
     * @param path the file path to check
     * @return true if the file is a macOS Alias, false otherwise
     * @throws IOException if an I/O error occurs while checking the file
     *
     * @implNote This method checks both the FinderInfo extended attribute (primary method)
     *           and the filename suffix (fallback heuristic). On non-macOS systems,
     *           this will always return false.
     */
    public static boolean isMacOSAlias(Path path) throws IOException {
        if (!System.getProperty("os.name").toLowerCase().contains("mac")) {
            return false;
        }
        try {
            byte[] finderInfo = (byte[]) Files.getAttribute(path, "xattr:com.apple.FinderInfo");
            if (finderInfo != null && finderInfo.length >= 32) {
                return finderInfo[0] == 'a' && finderInfo[1] == 'l' && finderInfo[2] == 'i' && finderInfo[3] == 's';
            }
        } catch (Exception e) {
            // Attribute not found or not supported
        }
        return path.getFileName().toString().endsWith(" alias"); // Fallback heuristic
    }

    /**
     * Copies a macOS Alias file to a new location while maintaining its reference.
     * <p>
     * If the alias points to a file within the source directory tree, the reference
     * will be updated to point to the corresponding location in the target directory.
     * External references are preserved as-is.
     *
     * @param source the source alias file to copy
     * @param target the destination path for the new alias
     * @param sourceBase the base source directory (for rebasing internal references)
     * @param targetBase the base target directory (for rebasing internal references)
     * @param copyOptions set of copy options (REPLACE_EXISTING is respected)
     * @param recordCallback callback to notify when copy is complete
     * @throws IOException if the alias cannot be copied or if target file operations fail
     *
     * @see StandardCopyOption#REPLACE_EXISTING
     */
    public static void copyMacOsAlias(Path source, Path target, Path sourceBase, Path targetBase,
                                      Set<CopyOption> copyOptions, Consumer<Path> recordCallback) throws IOException {
        Path originalTarget = resolveMacOsAliasTarget(source);
        Path finalTarget;

        if (originalTarget != null && originalTarget.startsWith(sourceBase)) {
            Path relativePath = sourceBase.relativize(originalTarget);
            finalTarget = targetBase.resolve(relativePath);
            logger.debug("Rebasing Alias {} from {} to {}", source, originalTarget, finalTarget);
        } else {
            finalTarget = originalTarget != null ? originalTarget : source;
            logger.debug("Preserving external Alias {} as {}", source, finalTarget);
        }

        try {
            // Respect REPLACE_EXISTING from copyOptions
            if (Files.exists(target) && copyOptions.contains(StandardCopyOption.REPLACE_EXISTING)) {
                logger.debug("Target {} exists, deleting due to REPLACE_EXISTING option", target);
                Files.delete(target);
            }
            Files.createSymbolicLink(target, finalTarget);
            logger.debug("Created symlink {} -> {}", target, finalTarget);
            recordCallback.accept(source);
        } catch (Exception e) {
            throw new IOException("Error copying Alias: " + e.getMessage(), e);
        }
    }

    /**
     * Resolves the original target of a macOS Alias file.
     *
     * @param aliasPath the path to the alias file
     * @return the resolved target path, or null if resolution fails
     * @throws IOException if resolution fails or if the alias is invalid
     *
     * @implNote This method uses macOS's built-in AppleScript integration to resolve
     *           the alias target. The resolution has a 5-second timeout.
     */
    public static Path resolveMacOsAliasTarget(Path aliasPath) throws IOException {
        if (System.getProperty("os.name").toLowerCase().contains("mac")) {
            try {
                // Corrected AppleScript to resolve the Alias target
                ProcessBuilder pb = new ProcessBuilder("osascript", "-e",
                        "tell application \"Finder\" to get POSIX path of (original item of alias file (POSIX file \"" + aliasPath.toString() + "\") as alias)");
                Process p = pb.start();
                try (BufferedReader reader = new BufferedReader(new InputStreamReader(p.getInputStream()))) {
                    String targetPath = reader.readLine();
                    if (targetPath != null && !targetPath.isEmpty()) {
                        Path resolvedPath = Paths.get(targetPath);
                        logger.debug("Resolved Alias {} to {}", aliasPath, resolvedPath);
                        return resolvedPath;
                    }
                }
                p.waitFor(5, TimeUnit.SECONDS);
            } catch (Exception e) {
                logger.warn("Failed to resolve Alias target via osascript: {}", e.getMessage());
            }
        }
        logger.warn("Could not resolve Alias target for {}; defaulting to file copy", aliasPath);
        return null;
    }
}