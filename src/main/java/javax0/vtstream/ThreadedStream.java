package javax0.vtstream;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.*;
import java.util.stream.*;

import static javax0.vtstream.Command.deleted;

@SuppressWarnings("NullableProblems")
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
        source.forEach(
                t -> {
                    Thread.startVirtualThread(() -> {
                        final var r = calculate(t);
                        if (!r.isDeleted()) {
                            result.add(r.result());
                        } else {
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
            //noinspection unchecked
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
        //noinspection unchecked
        return filteredStream(new Command.Filter<>((Predicate<T>) predicate));
    }

    private ThreadedStream<T> filteredStream(Command<T, T> command) {
        return new ThreadedStream<>(command, this, source);
    }


    public <R> ThreadedStream<R> map(Function<? super T, ? extends R> mapper) {
        //noinspection unchecked
        return new ThreadedStream<>((Command<R, ?>) new Command.Map<>((Function<T, R>) mapper), this, source);
    }

    @Override
    public IntStream mapToInt(ToIntFunction<? super T> mapper) {
        return toStream().mapToInt(mapper);
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
        return toStream().flatMap(mapper);
    }

    @Override
    public IntStream flatMapToInt(Function<? super T, ? extends IntStream> mapper) {
        return toStream().flatMapToInt(mapper);
    }

    @Override
    public LongStream flatMapToLong(Function<? super T, ? extends LongStream> mapper) {
        return toStream().flatMapToLong(mapper);
    }

    @Override
    public DoubleStream flatMapToDouble(Function<? super T, ? extends DoubleStream> mapper) {
        return toStream().flatMapToDouble(mapper);
    }

    @Override
    public ThreadedStream<T> distinct() {
        return filteredStream(new Command.Distinct<>());
    }

    @Override
    public Stream<T> sorted() {
        return toStream().sorted();
    }

    @Override
    public Stream<T> sorted(Comparator<? super T> comparator) {
        return toStream().sorted(comparator);
    }

    @Override
    public Stream<T> peek(Consumer<? super T> action) {
        return filteredStream(new Command.Peek<>(action));
    }


    @Override
    public ThreadedStream<T> limit(long maxSize) {
        return filteredStream(new Command.Limit<>(maxSize));
    }


    @Override
    public Stream<T> skip(long n) {
        return filteredStream(new Command.Skip<>(n));
    }


    @Override
    public void forEach(Consumer<? super T> action) {
        toStream().forEach(action);
    }


    @Override
    public void forEachOrdered(Consumer<? super T> action) {
        toStream().forEachOrdered(action);
    }


    @Override
    public Object[] toArray() {
        return toStream().toArray();
    }


    @Override
    public <A> A[] toArray(IntFunction<A[]> generator) {
        return toStream().toArray(generator);
    }


    @Override
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
        return filteredStream(new Command.AnyMatch<>(predicate)).toStream().findAny().isPresent();
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
        return filteredStream(new Command.FindFirst<>()).toStream().findFirst();
    }


    @Override
    public Optional<T> findAny() {
        return filteredStream(new Command.FindAny<>()).toStream().findAny();
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
        return new ThreadedStream<>(new Command.NoOp<>(), this, source, closeHandler);
    }


    @Override
    public void close() {
        if (downstream != null) {
            downstream.close();
        }
        closeHandler.run();
    }
}
