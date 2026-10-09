package com.salesforce.einstein.hierarchy.spi;

/** Globally unique 64-bit ids for spaces, nodes and move jobs, made without a database round trip. */
@FunctionalInterface
public interface IdGenerator {
    long nextId();
}
