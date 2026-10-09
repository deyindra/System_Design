package com.salesforce.einstein.hierarchy.domain;

import java.util.Locale;

/** A restriction on a node applies to its whole subtree. Editing also requires passing every view restriction. */
public enum RestrictionOp {
    VIEW, EDIT;

    public String db() {
        return name().toLowerCase(Locale.ROOT);
    }

    public static RestrictionOp parse(String s) {
        try {
            return valueOf(s.trim().toUpperCase(Locale.ROOT));
        } catch (IllegalArgumentException e) {
            throw new InvalidRequestException("op must be view or edit");
        }
    }
}
