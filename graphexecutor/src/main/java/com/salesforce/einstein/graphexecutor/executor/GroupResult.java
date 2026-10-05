package com.salesforce.einstein.graphexecutor.executor;

/**
 * Outcome of processing one {@link TaskGroup}: exactly one of {@code value} or {@code error} is meaningful.
 * A failed group never affects the others, so results are reported per group rather than as one
 * all-or-nothing outcome.
 */
public record GroupResult<T, R>(TaskGroup<T> group, R value, Throwable error) {

    public static <T, R> GroupResult<T, R> success(TaskGroup<T> group, R value) {
        return new GroupResult<>(group, value, null);
    }

    public static <T, R> GroupResult<T, R> failure(TaskGroup<T> group, Throwable error) {
        return new GroupResult<>(group, null, error);
    }

    public boolean isSuccess() {
        return error == null;
    }
}
