package com.salesforce.einstein.hierarchy.domain;

import java.util.Map;
import java.util.Set;

/**
 * Which node types may sit under which. Confluence: a space holds pages and folders. Jira: a project (space) holds
 * epics, an epic holds stories, a story holds sub-tasks, and a sub-task holds nothing.
 */
public final class TypeRules {
    public static final String SPACE = "space";
    private static final Map<String, Set<String>> ALLOWED = Map.of(
            SPACE, Set.of("page", "folder", "epic"),
            "folder", Set.of("page", "folder"),
            "page", Set.of("page"),
            "epic", Set.of("story"),
            "story", Set.of("subtask"),
            "subtask", Set.of());

    private TypeRules() {
    }

    public static boolean isUnknown(String type) {
        return !ALLOWED.containsKey(type);
    }

    public static void checkChild(String parentType, String childType) {
        if (isUnknown(childType) || SPACE.equals(childType)) {
            throw new InvalidRequestException("unknown node type '" + childType + "'");
        }
        if (!ALLOWED.getOrDefault(parentType, Set.of()).contains(childType)) {
            throw new InvalidRequestException("a " + childType + " cannot be placed under a " + parentType);
        }
    }
}
