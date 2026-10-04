package com.salesforce.einstein.tagging.api;

import com.salesforce.einstein.tagging.domain.InvalidRequestException;
import com.salesforce.einstein.tagging.domain.TenantInfo;
import org.springframework.core.MethodParameter;
import org.springframework.lang.Nullable;
import org.springframework.web.bind.support.WebDataBinderFactory;
import org.springframework.web.context.request.NativeWebRequest;
import org.springframework.web.method.support.HandlerMethodArgumentResolver;
import org.springframework.web.method.support.ModelAndViewContainer;

import java.util.regex.Pattern;

/** Builds a {@link Caller} for any controller parameter of that type. */
public final class CallerArgumentResolver implements HandlerMethodArgumentResolver {
    public static final String TENANT_HEADER = "X-Tenant-Id";
    public static final String ACTOR_HEADER = "X-Actor-Id";
    private static final Pattern ACTOR = Pattern.compile("[A-Za-z0-9._@:-]{1,64}");

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
        String actor = request.getHeader(ACTOR_HEADER);
        if (actor == null) {
            actor = "anonymous";
        } else if (!ACTOR.matcher(actor).matches()) {
            throw new InvalidRequestException(ACTOR_HEADER + " must match " + ACTOR.pattern());
        }
        return new Caller(TenantInfo.requireValidId(tenant), actor);
    }
}
