package com.salesforce.einstein.tagging.spi;

/** Port for globally unique, roughly time-ordered 64-bit ids (tag ids). */
public interface IdGenerator {
    long nextId();
}
