package javax0.vtstream;

import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.Spliterator;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BinaryOperator;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.IntFunction;
import java.util.function.Predicate;
import java.util.stream.Stream;

public class Threaded<T> {

    private final Command<T, ?> command;
    private final Threaded<?> downstream;
    private final Stream<?> source;

    private Threaded(Command<T, ?> command, Threaded<?> downstream, Stream<?> source) {
        this.command = command;
        this.downstream = downstream;
        this.source = source;
    }

    public static <K> Threaded<K> threaded(Stream<K> source) {
        return new Threaded<>(null, null, source);
    }

    private final AtomicInteger c = new AtomicInteger(1);

    public Stream<T> toStream() {
        if (source.isParallel()) {
            return toUnorderedStream();
        } else {
            return toOrderedStream();
        }
    }

    public Stream<T> toUnorderedStream() {
        ExecutorService executor = Executors.newThreadExecutor((r) -> Thread.builder()
            .virtual()
            .task(r)
            .build());
        final List<T> result = Collections.synchronizedList(new LinkedList<>());
        final List<Future<?>> futures = new LinkedList<>();
        source.spliterator().forEachRemaining(
            t -> futures.add(executor.submit(() -> {
                    final var r = calculate(t);
                    if (!r.isDeleted) {
                        result.add(r.result);
                    }
                }
            )));
        for (final var future : futures) {
            future.join();
        }
        return result.stream();
    }

    public Stream<T> toOrderedStream() {
        ExecutorService executor = Executors.newThreadExecutor((r) -> Thread.builder()
            .virtual()
            .task(r)
            .build());
        final var splitera = source.spliterator();
        final List<Future<Command.Result<T>>> result = Collections.synchronizedList(new LinkedList<>());
        splitera.forEachRemaining(t -> result.add(executor.submit(() -> calculate(t))));
        return result.stream().map(r -> {
            try {
                return r.get();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }).filter(r -> !r.isDeleted).map(r -> r.result);
    }


    Command.Result<T> calculate(Object value) {
        if (Thread.interrupted()) {
            return Command.RESULT_DELETED;
        }
        if (downstream == null) {
            return new Command.Result<>(false, (T) value);
        } else {
            final var result = downstream.calculate(value);
            if (result.isDeleted) {
                return Command.RESULT_DELETED;
            }
            return (Command.Result<T>) command.execute((T) result.result);
        }
    }

    public Threaded<T> filter(Predicate<? super T> predicate) {
        return new Threaded<T>(new Command.Filter<T>((Predicate<T>) predicate), this, source);
    }


    public <R> Threaded<R> map(Function<? super T, ? extends R> mapper) {
        return new Threaded<R>((Command<R, ?>) new Command.Map<T, R>((Function<T, R>) mapper), this, source);
    }

    public Threaded<T> distinct() {
        return new Threaded<T>(new Command.Distinct<T>(), this, source);
    }


    public Threaded<T> sorted() {
        throw new RuntimeException("not implemented");
    }


    public Threaded<T> sorted(Comparator<? super T> comparator) {
        throw new RuntimeException("not implemented");
    }


    public Threaded<T> peek(Consumer<? super T> action) {
        return new Threaded<T>(new Command.Peek<T>(action), this, source);
    }


    public Threaded<T> limit(long maxSize) {
        return new Threaded<T>(new Command.Limit<T>(maxSize), this, source);
    }


    public Threaded<T> skip(long n) {
        return new Threaded<T>(new Command.Skip<T>(n), this, source);
    }


    public void forEach(Consumer<? super T> action) {
        toStream().forEach(action);
    }


    public void forEachOrdered(Consumer<? super T> action) {
        toStream().forEachOrdered(action);
    }


    public Object[] toArray() {
        toStream().toArray();
    }


    public <A> A[] toArray(IntFunction<A[]> generator) {
        toStream().toArray(generator);
    }


    public T reduce(T identity, BinaryOperator<T> accumulator) {
        toStream().reduce(identity,accumulator);
    }


    public boolean anyMatch(Predicate<? super T> predicate) {
        return false;
    }


    public Optional<T> findAny() {
        return Optional.empty();
    }


    public Iterator<T> iterator() {
        throw new RuntimeException("not implemented");
    }


    public Spliterator<T> spliterator() {
        throw new RuntimeException("not implemented");
    }


    public boolean isParallel() {
        return true;
    }


    public Threaded<T> sequential() {
        throw new RuntimeException("not implemented");
    }


    public Threaded<T> parallel() {
        return this;
    }


    public Threaded<T> unordered() {
        return this;
    }


    public Threaded<T> onClose(Runnable closeHandler) {
        throw new RuntimeException("not implemented");
    }


    public void close() {

    }
}
