package com.salesforce.einstein.tagging.config;

import com.salesforce.einstein.tagging.api.CallerArgumentResolver;
import com.salesforce.einstein.tagging.domain.Consistency;
import com.salesforce.einstein.tagging.domain.InvalidRequestException;
import com.salesforce.einstein.tagging.domain.TrendRank;
import com.salesforce.einstein.tagging.domain.TrendWindow;
import org.springframework.context.annotation.Configuration;
import org.springframework.format.FormatterRegistry;
import org.springframework.web.method.support.HandlerMethodArgumentResolver;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

import java.util.List;
import java.util.Locale;

@Configuration(proxyBeanMethods = false)
public class WebConfiguration implements WebMvcConfigurer {
    @Override
    public void addArgumentResolvers(List<HandlerMethodArgumentResolver> resolvers) {
        resolvers.add(new CallerArgumentResolver());
    }

    /** {@code ?consistency=session} as well as {@code SESSION}; {@code ?window=24h}, {@code ?rank=rising}. */
    @Override
    public void addFormatters(FormatterRegistry registry) {
        registry.addConverter(String.class, Consistency.class, s -> {
            try {
                return Consistency.valueOf(s.trim().toUpperCase(Locale.ROOT));
            } catch (IllegalArgumentException e) {
                throw new InvalidRequestException("consistency must be STRONG, SESSION or EVENTUAL");
            }
        });
        registry.addConverter(String.class, TrendWindow.class, TrendWindow::parse);
        registry.addConverter(String.class, TrendRank.class, s -> {
            try {
                return TrendRank.valueOf(s.trim().toUpperCase(Locale.ROOT));
            } catch (IllegalArgumentException e) {
                throw new InvalidRequestException("rank must be POPULAR or RISING");
            }
        });
    }
}
