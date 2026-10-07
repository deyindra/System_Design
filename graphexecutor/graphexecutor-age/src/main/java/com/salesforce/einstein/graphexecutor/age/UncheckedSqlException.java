package com.salesforce.einstein.graphexecutor.age;

import java.io.Serial;
import java.sql.SQLException;

/** A {@link SQLException} from a store, rethrown unchecked because the store interfaces declare none. */
public final class UncheckedSqlException extends RuntimeException {
    @Serial
    private static final long serialVersionUID = 1L;

    public UncheckedSqlException(SQLException cause) {
        super(cause.getMessage(), cause);
    }

    @Override
    public synchronized SQLException getCause() {
        return (SQLException) super.getCause();
    }
}
