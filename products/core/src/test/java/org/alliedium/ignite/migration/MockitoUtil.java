package org.alliedium.ignite.migration;

import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Stubber;

import java.util.function.Consumer;

public class MockitoUtil {

    public static Stubber doAnswerVoid(Consumer<InvocationOnMock> consumer) {
        return Mockito.doAnswer(invocationOnMock -> {
            consumer.accept(invocationOnMock);
            return void.class;
        });
    }
}
