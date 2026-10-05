package com.salesforce.einstein.webcrawler.parse;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** {@code url(...)} and {@code @import} references inside CSS: background images, fonts, nested sheets. */
public final class CssParser {
    private CssParser() { }

    private static final Pattern URL = Pattern.compile("url\\(\\s*(['\"]?)([^'\")]+)\\1\\s*\\)", Pattern.CASE_INSENSITIVE);
    private static final Pattern IMPORT = Pattern.compile("@import\\s+(['\"])([^'\"]+)\\1", Pattern.CASE_INSENSITIVE);

    public static List<String> urls(String css) {
        List<String> out = new ArrayList<>();
        for (Pattern p : new Pattern[]{URL, IMPORT}) {
            Matcher m = p.matcher(css);
            while (m.find()) {
                String u = m.group(2).strip();
                if (!u.startsWith("data:")) out.add(u);
            }
        }
        return out;
    }
}
