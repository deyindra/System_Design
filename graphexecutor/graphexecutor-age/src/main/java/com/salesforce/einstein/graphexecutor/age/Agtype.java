package com.salesforce.einstein.graphexecutor.age;

import java.sql.SQLException;
import java.util.Collection;
import java.util.Map;

import org.postgresql.util.PGobject;

/**
 * AGE's {@code agtype} is JSON plus graph types. Cypher parameters are passed as one agtype map, and
 * returned keys come back as agtype strings ({@code "a"}, quoted); this writes the one and reads the other.
 * Only what the stores use: strings, numbers, lists and maps.
 */
final class Agtype {

    private Agtype() {
    }

    /** The Cypher parameter map {@code params}, as a JDBC parameter of type agtype. */
    static PGobject params(Map<String, ?> params) throws SQLException {
        PGobject object = new PGobject();
        object.setType("agtype");
        object.setValue(json(params));
        return object;
    }

    static String json(Object value) {
        StringBuilder out = new StringBuilder();
        write(value, out);
        return out.toString();
    }

    private static void write(Object value, StringBuilder out) {
        if (value instanceof String string) {
            quote(string, out);
        } else if (value instanceof Integer || value instanceof Long) {
            out.append(value);
        } else if (value instanceof Collection<?> list) {
            out.append('[');
            String separator = "";
            for (Object item : list) {
                out.append(separator);
                write(item, out);
                separator = ",";
            }
            out.append(']');
        } else if (value instanceof Map<?, ?> map) {
            out.append('{');
            String separator = "";
            for (Map.Entry<?, ?> entry : map.entrySet()) {
                out.append(separator);
                quote(entry.getKey().toString(), out);
                out.append(':');
                write(entry.getValue(), out);
                separator = ",";
            }
            out.append('}');
        } else {
            throw new IllegalArgumentException("not an agtype parameter: " + value);
        }
    }

    private static void quote(String string, StringBuilder out) {
        out.append('"');
        for (int i = 0; i < string.length(); i++) {
            char c = string.charAt(i);
            switch (c) {
                case '"' -> out.append("\\\"");
                case '\\' -> out.append("\\\\");
                case '\n' -> out.append("\\n");
                case '\r' -> out.append("\\r");
                case '\t' -> out.append("\\t");
                default -> {
                    if (c < 0x20) {
                        out.append(String.format("\\u%04x", (int) c));
                    } else {
                        out.append(c);
                    }
                }
            }
        }
        out.append('"');
    }

    /** An agtype string as returned by a query ({@code "a\"b"}); null for SQL NULL or agtype {@code null}. */
    static String string(String agtype) {
        if (agtype == null || agtype.equals("null")) {
            return null;
        }
        if (agtype.length() < 2 || agtype.charAt(0) != '"' || agtype.charAt(agtype.length() - 1) != '"') {
            throw new IllegalArgumentException("not an agtype string: " + agtype);
        }
        StringBuilder out = new StringBuilder(agtype.length() - 2);
        for (int i = 1; i < agtype.length() - 1; i++) {
            char c = agtype.charAt(i);
            if (c != '\\') {
                out.append(c);
                continue;
            }
            char escaped = agtype.charAt(++i);
            switch (escaped) {
                case 'n' -> out.append('\n');
                case 'r' -> out.append('\r');
                case 't' -> out.append('\t');
                case 'b' -> out.append('\b');
                case 'f' -> out.append('\f');
                case 'u' -> {
                    out.append((char) Integer.parseInt(agtype.substring(i + 1, i + 5), 16));
                    i += 4;
                }
                default -> out.append(escaped);   // \" \\ \/
            }
        }
        return out.toString();
    }
}
