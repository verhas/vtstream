package javax0.vtstream;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class TestThreaded {

    @Test
    void test() throws InterruptedException {
        AtomicInteger i = new AtomicInteger(3000);
        final var s = Threaded.threaded(Stream.of("a", "b", "c").parallel()).map(k ->
            {
                try {
                    Thread.sleep(i.getAndAdd(-1000));
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                return k + k;
            }
        ).toStream().collect(Collectors.joining(","));
        Assertions.assertEquals("aa,bb,cc", s);
    }
}
