package com.salesforce.einstein.hierarchy.domain;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TypeRulesTest {
    @Test
    void confluenceAndJiraShapes() {
        assertThatCode(() -> TypeRules.checkChild("space", "page")).doesNotThrowAnyException();
        assertThatCode(() -> TypeRules.checkChild("folder", "folder")).doesNotThrowAnyException();
        assertThatCode(() -> TypeRules.checkChild("space", "epic")).doesNotThrowAnyException();
        assertThatCode(() -> TypeRules.checkChild("epic", "story")).doesNotThrowAnyException();
        assertThatCode(() -> TypeRules.checkChild("story", "subtask")).doesNotThrowAnyException();
    }

    @Test
    void rejectsOtherShapes() {
        assertThatThrownBy(() -> TypeRules.checkChild("subtask", "subtask")).isInstanceOf(InvalidRequestException.class);
        assertThatThrownBy(() -> TypeRules.checkChild("page", "folder")).isInstanceOf(InvalidRequestException.class);
        assertThatThrownBy(() -> TypeRules.checkChild("epic", "page")).isInstanceOf(InvalidRequestException.class);
        assertThatThrownBy(() -> TypeRules.checkChild("folder", "space")).isInstanceOf(InvalidRequestException.class);
        assertThatThrownBy(() -> TypeRules.checkChild("space", "widget")).isInstanceOf(InvalidRequestException.class);
    }
}
