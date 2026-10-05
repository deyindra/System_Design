package com.salesforce.einstein.webcrawler;

import com.salesforce.einstein.webcrawler.store.ContentStore;
import com.salesforce.einstein.webcrawler.store.FileSystemContentStore;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import static org.junit.jupiter.api.Assertions.assertInstanceOf;

/** Choosing a different implementation for a port is a config change only. */
@SpringBootTest(properties = {
        "crawler.adapters.content-store=filesystem",
        "crawler.adapters.content-root=${java.io.tmpdir}/webcrawler-adapter-test"})
class AdapterSelectionTest {

    @Autowired ContentStore contents;

    @Test void contentStoreFollowsConfig() {
        assertInstanceOf(FileSystemContentStore.class, contents);
    }
}
