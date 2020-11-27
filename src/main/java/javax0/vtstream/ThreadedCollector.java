package javax0.vtstream;

import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collector;

public class ThreadedCollector<T, A, R, OR> implements Collector<T, A, OR> {

    private final Collector<T, A, OR> underlyingCollector;

    private final ThreadedStream<R> stream;

    private OR result;
    private boolean resultReady = false;
    private A accu;

    public OR getResult() {
        waitForTheFinalElement();
        return result;
    }

    private synchronized void waitForTheFinalElement() {
        try {
            while( ! resultReady ) {
                wait();
            }
        } catch (InterruptedException e) {
            if (!resultReady) {
                result = null;
            }
        }
    }

    private OR finish() {
        result = underlyingCollector.finisher().apply(accu);
        resultReady = true;
        synchronized (this){
            notify();
        }
        return result;
    }

    public ThreadedCollector(Collector<T, A, OR> underlyingCollector, ThreadedStream<R> stream) {
        this.underlyingCollector = underlyingCollector;
        this.stream = stream;
    }


    @Override
    public Supplier<A> supplier() {
        return underlyingCollector.supplier();
    }

    @Override
    public BiConsumer<A, T> accumulator() {
        return underlyingCollector.accumulator();
    }

    @Override
    public BinaryOperator<A> combiner() {
        return underlyingCollector.combiner();
    }

    @Override
    public Function<A, OR> finisher() {
        return underlyingCollector.finisher();
    }

    @Override
    public Set<Characteristics> characteristics() {
        return underlyingCollector.characteristics();
    }
}
