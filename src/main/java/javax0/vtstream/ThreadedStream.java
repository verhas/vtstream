package javax0.vtstream;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.*;
import java.util.stream.*;

import static javax0.vtstream.Command.deleted;

public class ThreadedStream<T> implements Stream<T> {

    private final Command<T, ?> command;
    private final ThreadedStream<?> downstream;
    private final Stream<?> source;

    private ThreadedStream(Command<T, ?> command, ThreadedStream<?> downstream, Stream<?> source) {
        this.command = command;
        this.downstream = downstream;
        this.source = source;
        this.closeHandler = () -> {
        };
    }

    private ThreadedStream(Command<T, ?> command, ThreadedStream<?> downstream, Stream<?> source, Runnable closeHandler) {
        this.command = command;
        this.downstream = downstream;
        this.source = source;
        this.closeHandler = closeHandler;
    }

    public static <K> ThreadedStream<K> threaded(Stream<K> source) {
        return new ThreadedStream<>(null, null, source);
    }

    private final AtomicInteger c = new AtomicInteger(1);

    private Stream<T> toStream() {
        if (source.isParallel()) {
            return toUnorderedStream();
        } else {
            return toOrderedStream();
        }
    }

    private Stream<T> toUnorderedStream() {
        final var result = Collections.synchronizedList(new LinkedList<T>());
        final AtomicInteger n = new AtomicInteger(0);
        source.spliterator().forEachRemaining(
                t -> {
                    Thread.startVirtualThread(() -> {
                        final var r = calculate(t);
                        if (!r.isDeleted()) {
                            result.add(r.result());
                        }else{
                            n.decrementAndGet();
                        }
                    });
                    n.incrementAndGet();
                }
        );
        while (result.isEmpty()) {
            Thread.yield();
        }
        return Stream.iterate(
                result.removeFirst(),
                x -> n.getAndDecrement() > 0,
                x -> {
                    while (n.get() > 0 && result.isEmpty()) {
                        Thread.yield();
                    }
                    return result.isEmpty() ? null : result.removeFirst();
                });
    }

    private Stream<T> toOrderedStream() {
        class R {
            Thread t;
            Command.Result<T> result;
        }
        final var results = Collections.synchronizedList(new LinkedList<R>());
        final AtomicInteger n = new AtomicInteger(0);
        source.forEach(
                t -> {
                    final var re = new R();
                    re.t =
                            Thread.startVirtualThread(() -> {
                                final var r = calculate(t);
                                if (!r.isDeleted()) {
                                    re.result = r;
                                }
                            });
                    results.add(re);
                    n.incrementAndGet();
                }
        );
        final var first = results.removeFirst();
        try {
            first.t.join();
        } catch (InterruptedException e) {
            first.result = deleted();
        }
        return Stream.iterate(
                first.result.result(),
                x -> n.getAndDecrement() > 0,
                x -> {
                    if (n.get() > 0) {
                        final var next = results.removeFirst();
                        try {
                            next.t.join();
                        } catch (InterruptedException e) {
                            next.result = deleted();
                        }
                        return next.result.isDeleted() ? null : next.result.result();
                    } else {
                        return null;
                    }
                });
    }


    Command.Result<T> calculate(Object value) {
        if (Thread.interrupted()) {
            return deleted();
        }
        if (downstream == null) {
            return new Command.Result<>((T) value);
        } else {
            final var result = downstream.calculate(value);
            if (result.isDeleted()) {
                return deleted();
            }
            //noinspection unchecked
            return (Command.Result<T>) command.execute((T) result.result());
        }
    }

    public Stream<T> filter(Predicate<? super T> predicate) {
        return new ThreadedStream<T>(
                new Command.Filter<T>((Predicate<T>) predicate),
                this,
                source);
    }


    public <R> ThreadedStream<R> map(Function<? super T, ? extends R> mapper) {
        return new ThreadedStream<R>((Command<R, ?>) new Command.Map<T, R>((Function<T, R>) mapper), this, source);
    }

    @Override
    public IntStream mapToInt(ToIntFunction<? super T> mapper) {
        return null;
    }

    @Override
    public LongStream mapToLong(ToLongFunction<? super T> mapper) {
        return toStream().mapToLong(mapper);
    }

    @Override
    public DoubleStream mapToDouble(ToDoubleFunction<? super T> mapper) {
        return toStream().mapToDouble(mapper);
    }

    @Override
    public <R> Stream<R> flatMap(Function<? super T, ? extends Stream<? extends R>> mapper) {
        return null;
    }

    @Override
    public IntStream flatMapToInt(Function<? super T, ? extends IntStream> mapper) {
        return null;
    }

    @Override
    public LongStream flatMapToLong(Function<? super T, ? extends LongStream> mapper) {
        return null;
    }

    @Override
    public DoubleStream flatMapToDouble(Function<? super T, ? extends DoubleStream> mapper) {
        return null;
    }

    public Stream<T> distinct() {
        return new ThreadedStream<T>(new Command.Distinct<T>(), this, source);
    }


    public Stream<T> sorted() {
        throw new RuntimeException("not implemented");
    }


    public Stream<T> sorted(Comparator<? super T> comparator) {
        throw new RuntimeException("not implemented");
    }


    public Stream<T> peek(Consumer<? super T> action) {
        return new ThreadedStream<T>(new Command.Peek<T>(action), this, source);
    }


    public Stream<T> limit(long maxSize) {
        return new ThreadedStream<T>(new Command.Limit<T>(maxSize), this, source);
    }


    public Stream<T> skip(long n) {
        return new ThreadedStream<T>(new Command.Skip<T>(n), this, source);
    }


    public void forEach(Consumer<? super T> action) {
        toStream().forEach(action);
    }


    public void forEachOrdered(Consumer<? super T> action) {
        toStream().forEachOrdered(action);
    }


    public Object[] toArray() {
        return toStream().toArray();
    }


    public <A> A[] toArray(IntFunction<A[]> generator) {
        return toStream().toArray(generator);
    }


    public T reduce(T identity, BinaryOperator<T> accumulator) {
        return toStream().reduce(identity, accumulator);
    }

    @Override
    public Optional<T> reduce(BinaryOperator<T> accumulator) {
        return toStream().reduce(accumulator);
    }

    @Override
    public <U> U reduce(U identity, BiFunction<U, ? super T, U> accumulator, BinaryOperator<U> combiner) {
        return toStream().reduce(identity, accumulator, combiner);
    }

    @Override
    public <R> R collect(Supplier<R> supplier, BiConsumer<R, ? super T> accumulator, BiConsumer<R, R> combiner) {
        return toStream().collect(supplier, accumulator, combiner);
    }

    @Override
    public <R, A> R collect(Collector<? super T, A, R> collector) {
        return toStream().collect(collector);
    }

    @Override
    public Optional<T> min(Comparator<? super T> comparator) {
        return toStream().min(comparator);
    }

    @Override
    public Optional<T> max(Comparator<? super T> comparator) {
        return toStream().max(comparator);
    }

    @Override
    public long count() {
        return toStream().count();
    }


    public boolean anyMatch(Predicate<? super T> predicate) {
        return new ThreadedStream<T>(new Command.AnyMatch<T>(predicate), this, source).toStream().findAny().isPresent();
    }

    @Override
    public boolean allMatch(Predicate<? super T> predicate) {
        return toStream().allMatch(predicate);
    }

    @Override
    public boolean noneMatch(Predicate<? super T> predicate) {
        return toStream().noneMatch(predicate);
    }

    @Override
    public Optional<T> findFirst() {
        return new ThreadedStream<T>(new Command.FindFirst<>(), downstream, source).toStream().findFirst();
    }


    @Override
    public Optional<T> findAny() {
        return findFirst();
    }


    @Override
    public Iterator<T> iterator() {
        return toStream().iterator();
    }


    @Override
    public Spliterator<T> spliterator() {
        return toStream().spliterator();
    }


    @Override
    public boolean isParallel() {
        return true;
    }


    @Override
    public Stream<T> sequential() {
        if (source.isParallel()) {
            return threaded(toStream().sequential());
        }
        return this;
    }


    @Override
    public Stream<T> parallel() {
        if (source.isParallel()) {
            return this;
        }
        return threaded(toStream().parallel());
    }


    @Override
    public Stream<T> unordered() {
        return this;
    }


    private final Runnable closeHandler;

    @Override
    public Stream<T> onClose(Runnable closeHandler) {
        return new ThreadedStream<T>(new Command.NoOp<>(), this, source, closeHandler);
    }


    @Override
    public void close() {
        if (downstream != null) {
            downstream.close();
        }
        closeHandler.run();
    }
}
