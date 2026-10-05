package com.salesforce.einstein.webcrawler;

import com.salesforce.einstein.webcrawler.store.ContentStore;
import com.salesforce.einstein.webcrawler.store.FileSystemContentStore;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertTrue;

class FileSystemContentStoreTest extends ContentStoreContract {
    @TempDir Path dir;

    @Override ContentStore store() { return new FileSystemContentStore(dir); }

    @Test void usesTheSharedKeyLayoutOnDisk() {
        store().put(HASH, BYTES);
        assertTrue(Files.exists(dir.resolve(HASH.substring(0, 2)).resolve(HASH)));
    }
}
