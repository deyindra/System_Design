package com.salesforce.einstein.webcrawler.store;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Optional;

/**
 * {@link ContentStore} on a directory: a local disk, or any shared filesystem every worker mounts (NFS, or a
 * managed file share on any cloud). Uses the {@link ContentStore#key} layout, so a directory can be synced to and
 * from an object-store bucket unchanged.
 *
 * <p>Writes go to a temp file and are moved into place atomically, so readers never see a partial blob and two
 * workers writing the same hash both succeed with identical bytes.
 */
public final class FileSystemContentStore implements ContentStore {

    private final Path root;

    public FileSystemContentStore(Path root) {
        this.root = root;
        try { Files.createDirectories(root); } catch (IOException e) { throw new UncheckedIOException(e); }
    }

    @Override public boolean put(String contentHash, byte[] bytes) {
        Path target = path(contentHash);
        if (Files.exists(target)) return false;
        try {
            Files.createDirectories(target.getParent());
            Path tmp = Files.createTempFile(target.getParent(), contentHash, ".tmp");
            try {
                Files.write(tmp, bytes);
                try {
                    Files.move(tmp, target, StandardCopyOption.ATOMIC_MOVE);
                } catch (AtomicMoveNotSupportedException e) {
                    Files.move(tmp, target);
                }
                return true;
            } catch (FileAlreadyExistsException e) {
                return false;                       // another worker won; same hash, same bytes
            } finally {
                Files.deleteIfExists(tmp);
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override public Optional<byte[]> get(String contentHash) {
        try {
            return Optional.of(Files.readAllBytes(path(contentHash)));
        } catch (NoSuchFileException e) {
            return Optional.empty();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override public boolean contains(String contentHash) { return Files.exists(path(contentHash)); }

    private Path path(String contentHash) { return root.resolve(ContentStore.key(contentHash)); }
}
