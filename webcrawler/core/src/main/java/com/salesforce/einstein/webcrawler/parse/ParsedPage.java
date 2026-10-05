package com.salesforce.einstein.webcrawler.parse;

import java.util.List;

/**
 * @param baseUri   what relative links resolve against: the page URL, or its {@code <base href>}
 * @param canonical {@code <link rel=canonical>} if present (raw)
 * @param noFollow  {@code <meta name=robots content=nofollow>}: record edges, do not enqueue them
 */
public record ParsedPage(String baseUri, String title, String canonical, boolean noFollow, boolean noIndex,
                         List<ExtractedLink> links) { }
