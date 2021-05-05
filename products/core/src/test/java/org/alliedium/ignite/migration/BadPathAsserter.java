package org.alliedium.ignite.migration;

import org.junit.Assert;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

public class BadPathAsserter {

    private final Action action;
    private final List<Consumer<Exception>> assertions;

    public BadPathAsserter(Action action) {
        this.action = action;
        this.assertions = new ArrayList<>();
    }

    public void assertExceptionThrownAndMessageContains(String message) {
        assertions.add(exception -> Assert.assertTrue(exception.getMessage().contains(message)));
    }

    public void invokeActionApplyAssertions() {
        try {
            action.invoke();
        } catch (Exception e) {
            assertions.forEach(assertion -> assertion.accept(e));
            return;
        }

        Assert.fail("This is bad path test and it did not fail");
    }
}
