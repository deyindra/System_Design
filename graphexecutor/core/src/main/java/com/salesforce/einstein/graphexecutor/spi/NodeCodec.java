package com.salesforce.einstein.graphexecutor.spi;

import java.util.Objects;
import java.util.function.Function;

/**
 * How a node is written outside the JVM: a graph database stores it as a key, and workers on other machines
 * read that key back. {@code decode(encode(n))} must equal {@code n}, and equal nodes must have equal keys.
 *
 * @param <T> node type
 */
public interface NodeCodec<T> {

    String encode(T node);

    T decode(String key);

    static NodeCodec<String> strings() {
        return of(Function.identity(), Function.identity());
    }

    static <T> NodeCodec<T> of(Function<? super T, String> encode, Function<String, ? extends T> decode) {
        Objects.requireNonNull(encode, "encode");
        Objects.requireNonNull(decode, "decode");
        return new NodeCodec<>() {
            @Override
            public String encode(T node) {
                return encode.apply(node);
            }

            @Override
            public T decode(String key) {
                return decode.apply(key);
            }
        };
    }
}
