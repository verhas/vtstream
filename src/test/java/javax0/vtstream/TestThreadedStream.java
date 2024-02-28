package javax0.vtstream;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class TestThreadedStream {

    @Test
    @DisplayName("Simple parallel execution")
    void testParallelExecution() {
        AtomicInteger i = new AtomicInteger(3000);
        final var s = ThreadedStream.threaded(Stream.of("a", "b", "c").parallel()).map(k ->
                {
                    try {
                        Thread.sleep(i.getAndAdd(-1000));
                    } catch (InterruptedException ignore) {
                    }
                    return k + k;
                }
        ).collect(Collectors.toSet());
        Assertions.assertEquals(Set.of("aa", "bb", "cc"), s);
    }

    @Test
    @DisplayName("Simple serial execution")
    void testSerialExecution() {
        AtomicInteger i = new AtomicInteger(3000);
        final var s = ThreadedStream.threaded(Stream.of("a", "b", "c")).map(k ->
                {
                    try {
                        Thread.sleep(i.getAndAdd(-1000));
                    } catch (InterruptedException ignore) {
                    }
                    return k + k;
                }
        ).collect(Collectors.toList());
        Assertions.assertEquals(List.of("aa", "bb", "cc"), s);
    }

    @Test
    @DisplayName("Test filtering")
    void testFilterExecution() {
        AtomicInteger i = new AtomicInteger(3000);
        final var s = ThreadedStream.threaded(Stream.of("a", "b", "c").parallel())
                .map(k -> k + k)
                .filter(k-> k.startsWith("a"))
                .findAny();
        Assertions.assertEquals("aa", s.get());
    }

}
