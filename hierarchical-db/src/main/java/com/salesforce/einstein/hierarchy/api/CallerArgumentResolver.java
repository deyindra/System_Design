package com.salesforce.einstein.hierarchy.api;

import com.salesforce.einstein.hierarchy.domain.Caller;
import com.salesforce.einstein.hierarchy.domain.InvalidRequestException;
import org.springframework.core.MethodParameter;
import org.springframework.lang.Nullable;
import org.springframework.web.bind.support.WebDataBinderFactory;
import org.springframework.web.context.request.NativeWebRequest;
import org.springframework.web.method.support.HandlerMethodArgumentResolver;
import org.springframework.web.method.support.ModelAndViewContainer;

import java.util.Arrays;
import java.util.Set;
import java.util.UUID;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/** Builds a {@link Caller} for any controller parameter of that type, from headers set by the gateway. */
public final class CallerArgumentResolver implements HandlerMethodArgumentResolver {
    public static final String TENANT_HEADER = "X-Tenant-Id";
    public static final String ACTOR_HEADER = "X-Actor-Id";
    public static final String GROUPS_HEADER = "X-Actor-Groups";
    private static final Pattern NAME = Pattern.compile("[A-Za-z0-9._@:-]{1,64}");
    private static final int MAX_GROUPS = 100;

    @Override
    public boolean supportsParameter(MethodParameter parameter) {
        return parameter.getParameterType() == Caller.class;
    }

    @Override
    public Caller resolveArgument(MethodParameter parameter, @Nullable ModelAndViewContainer mav,
                                  NativeWebRequest request, @Nullable WebDataBinderFactory binderFactory) {
        String tenant = request.getHeader(TENANT_HEADER);
        if (tenant == null) {
            throw new InvalidRequestException(TENANT_HEADER + " header is required");
        }
        UUID tenantId;
        try {
            tenantId = UUID.fromString(tenant.trim());
        } catch (IllegalArgumentException e) {
            throw new InvalidRequestException(TENANT_HEADER + " must be a UUID");
        }
        String actor = request.getHeader(ACTOR_HEADER);
        if (actor == null) {
            actor = "anonymous";
        } else if (!NAME.matcher(actor).matches()) {
            throw new InvalidRequestException(ACTOR_HEADER + " must match " + NAME.pattern());
        }
        return new Caller(tenantId, actor, groups(request.getHeader(GROUPS_HEADER)));
    }

    private static Set<String> groups(@Nullable String header) {
        if (header == null || header.isBlank()) {
            return Set.of();
        }
        Set<String> groups = Arrays.stream(header.split(",")).map(String::trim).filter(g -> !g.isEmpty())
                .collect(Collectors.toSet());
        if (groups.size() > MAX_GROUPS) {
            throw new InvalidRequestException(GROUPS_HEADER + " lists more than " + MAX_GROUPS + " groups");
        }
        for (String g : groups) {
            if (!NAME.matcher(g).matches()) {
                throw new InvalidRequestException(GROUPS_HEADER + " entries must match " + NAME.pattern());
            }
        }
        return groups;
    }
}
