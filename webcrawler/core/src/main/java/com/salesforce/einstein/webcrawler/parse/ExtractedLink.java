package com.salesforce.einstein.webcrawler.parse;

import com.salesforce.einstein.webcrawler.model.ResourceType;

/** A reference found in a document, before normalization. {@code raw} is exactly what the author wrote. */
public record ExtractedLink(String raw, ResourceType type) { }
