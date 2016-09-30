package io.digdag.cli.client;

import com.google.common.io.ByteStreams;
import com.google.inject.Inject;
import io.digdag.cli.StdOut;
import io.digdag.cli.YamlMapper;
import io.digdag.client.config.Config;
import io.digdag.core.archive.ProjectArchive;
import io.digdag.core.archive.ProjectArchiveLoader;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.apache.commons.compress.archivers.tar.TarConstants;
import org.apache.commons.compress.compressors.gzip.GzipCompressorOutputStream;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermission;
import java.util.HashSet;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Locale.ENGLISH;

class Archiver
{
    private final PrintStream out;
    private final ProjectArchiveLoader projectLoader;
    private final YamlMapper yamlMapper;

    @Inject
    Archiver(@StdOut PrintStream out, ProjectArchiveLoader projectLoader, YamlMapper yamlMapper)
    {
        this.out = out;
        this.projectLoader = projectLoader;
        this.yamlMapper = yamlMapper;
    }

    private static void listFilesRecursively(Path projectPath, Path targetDir, ProjectArchive.PathConsumer consumer, Set<String> listed)
            throws IOException
    {
        try (DirectoryStream<Path> ds = Files.newDirectoryStream(targetDir, Archiver::rejectDotFiles)) {
            for (Path path : ds) {
                String resourceName = realPathToResourceName(projectPath, path);
                if (listed.add(resourceName)) {
                    consumer.accept(resourceName, path);
                    if (Files.isDirectory(path)) {
                        listFilesRecursively(projectPath, path, consumer, listed);
                    }
                }
            }
        }
    }

    private static String realPathToResourceName(Path projectPath, Path realPath)
    {
        checkArgument(projectPath.isAbsolute(), "project path must be absolute: %s", projectPath);
        checkArgument(realPath.isAbsolute(), "real path must be absolute: %s", realPath);

        if (!realPath.startsWith(projectPath)) {
            throw new IllegalArgumentException(String.format(ENGLISH,
                    "Given path '%s' is outside of project directory '%s'",
                    realPath, projectPath));
        }
        Path relative = projectPath.relativize(realPath);

        // resource name must be separated by '/'. Resource names are used as a part of
        // workflow name later using following resourceNameToWorkflowName method.
        // See also ProjectArchiveLoader.loadWorkflowFile.
        return relative.toString().replace(File.separatorChar, '/');
    }

    private static boolean rejectDotFiles(Path target)
    {
        return !target.getFileName().toString().startsWith(".");
    }

    void createArchive(Path projectPath, Path output, Config overwriteParams)
            throws IOException
    {
        out.println("Creating " + output + "...");

        try (TarArchiveOutputStream tar = new TarArchiveOutputStream(new GzipCompressorOutputStream(Files.newOutputStream(output)))) {
            // default mode for file names longer than 100 bytes is throwing an exception (LONGFILE_ERROR)
            tar.setLongFileMode(TarArchiveOutputStream.LONGFILE_POSIX);

            listFilesRecursively(projectPath, projectPath, (resourceName, absPath) -> {
                if (!Files.isDirectory(absPath)) {
                    out.println("  Archiving " + resourceName);

                    TarArchiveEntry e = buildTarArchiveEntry(projectPath, absPath, resourceName);
                    tar.putArchiveEntry(e);
                    if (e.isSymbolicLink()) {
                        out.println("    symlink -> " + e.getLinkName());
                    }
                    else {
                        try (InputStream in = Files.newInputStream(absPath)) {
                            ByteStreams.copy(in, tar);
                        }
                        tar.closeArchiveEntry();
                    }
                }
            }, new HashSet<>());
        }
    }

    private TarArchiveEntry buildTarArchiveEntry(Path projectPath, Path absPath, String name)
            throws IOException
    {
        TarArchiveEntry e;
        if (Files.isSymbolicLink(absPath)) {
            e = new TarArchiveEntry(name, TarConstants.LF_SYMLINK);
            Path rawDest = Files.readSymbolicLink(absPath);
            Path normalizedAbsDest = absPath.getParent().resolve(rawDest).normalize();

            if (!normalizedAbsDest.startsWith(projectPath)) {
                throw new IllegalArgumentException(String.format(ENGLISH,
                        "Invalid symbolic link: Given path '%s' is outside of project directory '%s'", normalizedAbsDest, projectPath));
            }

            // absolute path will be invalid on a server. convert it to a relative path
            Path normalizedRelativeDest = absPath.getParent().relativize(normalizedAbsDest);

            String linkName = normalizedRelativeDest.toString();

            // TarArchiveEntry(File) does this normalization but setLinkName doesn't. So do it here:
            linkName = linkName.replace(File.separatorChar, '/');
            e.setLinkName(linkName);
        }
        else {
            e = new TarArchiveEntry(absPath.toFile(), name);
            try {
                int mode = 0;
                for (PosixFilePermission perm : Files.getPosixFilePermissions(absPath)) {
                    switch (perm) {
                        case OWNER_READ:
                            mode |= 0400;
                            break;
                        case OWNER_WRITE:
                            mode |= 0200;
                            break;
                        case OWNER_EXECUTE:
                            mode |= 0100;
                            break;
                        case GROUP_READ:
                            mode |= 0040;
                            break;
                        case GROUP_WRITE:
                            mode |= 0020;
                            break;
                        case GROUP_EXECUTE:
                            mode |= 0010;
                            break;
                        case OTHERS_READ:
                            mode |= 0004;
                            break;
                        case OTHERS_WRITE:
                            mode |= 0002;
                            break;
                        case OTHERS_EXECUTE:
                            mode |= 0001;
                            break;
                        default:
                            // ignore
                    }
                }
                e.setMode(mode);
            }
            catch (UnsupportedOperationException ex) {
                // ignore custom mode
            }
        }
        return e;
    }
}
