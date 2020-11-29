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
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.BinaryOperator;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.IntFunction;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.function.ToDoubleFunction;
import java.util.function.ToIntFunction;
import java.util.function.ToLongFunction;
import java.util.stream.Collector;
import java.util.stream.DoubleStream;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import java.util.stream.Stream;

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
        ThreadGroup group = new ThreadGroup("stream");
        group.setMaxPriority(Thread.MAX_PRIORITY);
        group.setDaemon(true);
        ExecutorService executor = Executors.newThreadExecutor((r) -> Thread.builder()
            .group(group)
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

    private Stream<T> toOrderedStream() {
        ExecutorService executor = Executors.newThreadExecutor((r) -> Thread.builder()
            .virtual()
            .task(r)
            .build());
        final List<Future<Command.Result<T>>> result = Collections.synchronizedList(new LinkedList<>());
        source.spliterator().forEachRemaining(t -> result.add(executor.submit(() -> calculate(t))));
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

    public Stream<T> filter(Predicate<? super T> predicate) {
        return new ThreadedStream<T>(new Command.Filter<T>((Predicate<T>) predicate), this, source);
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
        return toStream().reduce(identity,accumulator,combiner);
    }

    @Override
    public <R> R collect(Supplier<R> supplier, BiConsumer<R, ? super T> accumulator, BiConsumer<R, R> combiner) {
        return toStream().collect(supplier,accumulator,combiner);
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
        return new ThreadedStream<T>(new Command.FindFirst<>(), downstream, this).toStream().findFirst();
    }


    public Optional<T> findAny() {
        return findFirst();
    }


    public Iterator<T> iterator() {
        return toStream().iterator();
    }


    public Spliterator<T> spliterator() {
        return toStream().spliterator();
    }


    public boolean isParallel() {
        return true;
    }


    public Stream<T> sequential() {
        if (source.isParallel()) {
            return threaded(toStream().sequential());
        }
        return this;
    }


    public Stream<T> parallel() {
        if (source.isParallel()) {
            return this;
        }
        return threaded(toStream().parallel());
    }


    public Stream<T> unordered() {
        return this;
    }


    private final Runnable closeHandler;

    public Stream<T> onClose(Runnable closeHandler) {
        return new ThreadedStream<T>(new Command.NoOp<>(), this, source, closeHandler);
    }


    public void close() {
        if (downstream != null) {
            downstream.close();
        }
        closeHandler.run();
    }
}
