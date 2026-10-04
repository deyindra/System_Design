package com.salesforce.einstein.tagging.api;

/**
 * Who is calling: the tenant (Atlassian cloudId) and the acting user or service.
 *
 * <p>Here they come from the {@code X-Tenant-Id} / {@code X-Actor-Id} headers set by the ingress, which
 * verifies the OIDC JWT and copies its claims. Clients are never trusted to name their own tenant.
 */
public record Caller(String tenantId, String actorId) {
}
