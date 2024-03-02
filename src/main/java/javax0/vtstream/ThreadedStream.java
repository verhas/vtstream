package javax0.vtstream;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.*;
import java.util.stream.*;

import static javax0.vtstream.Command.deleted;
import static javax0.vtstream.Command.exception;

@SuppressWarnings("NullableProblems")
public class ThreadedStream<T> implements Stream<T> {

    private final Command<T, ?> command;
    private final ThreadedStream<?> downstream;
    private final Stream<?> source;

    private long limit = -1;

    private boolean chained = false;
    private static final String MSG_STREAM_LINKED = "stream has already been operated upon or closed";

    private ThreadedStream(Command<T, ?> command, ThreadedStream<?> downstream, Stream<?> source) {
        this(command, downstream, source, () -> {
        });
    }

    private ThreadedStream(Command<T, ?> command, ThreadedStream<?> downstream, Stream<?> source, Runnable closeHandler) {
        this.command = command;
        this.downstream = downstream;
        if (downstream != null) {
            if (downstream.chained) {
                throw new IllegalStateException(MSG_STREAM_LINKED);
            }
            downstream.chained = true;
        }
        this.source = source;
        this.limit = downstream == null ? -1 : downstream.limit;
        this.closeHandler = closeHandler;
    }

    public static <K> ThreadedStream<K> threaded(Stream<K> source) {
        return new ThreadedStream<>(null, null, source);
    }


    public static class ThreadExecutionException extends RuntimeException {
        public ThreadExecutionException(Throwable cause) {
            super(cause);
        }
    }

    private Stream<T> toStream() {
        try {
            if (source.isParallel()) {
                return toUnorderedStream();
            } else {
                return toOrderedStream();
            }
        }finally {
            close();
        }
    }

    /**
     * Creates an unordered stream of processed elements from the source stream. This method leverages virtual threads
     * to process each element of the source stream concurrently. The results are stored in a synchronized list as they
     * become available, allowing for the immediate consumption of processed elements without waiting for all processing
     * threads to complete.
     * <p>
     * This approach facilitates a non-blocking, efficient streaming process where elements are emitted to the resulting
     * stream as soon as they are ready. The synchronization on the {@code results} list ensures thread safety, while the use of
     * an {@link AtomicInteger} allows for tracking the completion of elements without blocking stream consumption.
     * <p>
     * The returned stream filters out any elements marked as deleted (indicating processing failures or interruptions) and
     * maps the remaining {@link Command.Result} objects to their respective results. This method effectively demonstrates
     * an asynchronous, concurrent processing pattern within the stream API, optimizing throughput and reducing latency
     * for stream operations.
     * <p>
     * Note: The use of virtual threads requires the JVM to support Project Loom or a similar concurrency model. The method
     * employs a busy-wait loop to poll for available results, which may have implications for CPU usage in environments
     * with a high number of concurrent tasks.
     *
     * @return a new {@link Stream} of type {@code T}, consisting of elements processed concurrently and emitted as soon as
     * they become available, disregarding their original order in the source stream.
     */

    private Stream<T> toUnorderedStream() {
        final var result = Collections.synchronizedList(new LinkedList<Command.Result<T>>());
        final AtomicInteger n = new AtomicInteger(0);
        final Stream<?> limitedSource = limit >= 0 ? source.limit(limit) : source;
        limitedSource.forEach(
                t -> {
                    Thread.startVirtualThread(() -> result.add(calculate(t)));
                    n.incrementAndGet();
                });
        return IntStream.range(0, n.get())
                .mapToObj(i -> {
                    while (result.isEmpty()) {
                        Thread.yield();
                    }
                    return result.removeFirst();
                })
                .filter(f -> !f.isDeleted())
                .peek(r -> {
                    if (r.exception() != null) {
                        throw new ThreadExecutionException(r.exception());
                    }
                })
                .map(Command.Result::result);
    }


    private Stream<T> toOrderedStream() {
        class R {
            Thread t;
            volatile Command.Result<T> result;

            /**
             * Wait for the thread calculating the result to be finished. This method is blocking.
             * @param result the result to wait for
             */
            static void waitForResult(R result) {
                try {
                    result.t.join();
                } catch (InterruptedException e) {
                    result.result = deleted();
                }
            }
        }
        final var results = Collections.synchronizedList(new LinkedList<R>());

        final Stream<?> limitedSource = limit >= 0 ? source.limit(limit) : source;
        limitedSource.forEach(
                t -> {
                    final var re = new R();
                    results.add(re);
                    re.t = Thread.startVirtualThread(() -> {
                        re.result = calculate(t);
                    });
                }
        );

        return results.stream()
                .peek(R::waitForResult)
                .map(f -> f.result)
                .peek(r -> {
                            if (r.exception() != null) {
                                throw new ThreadExecutionException(r.exception());
                            }
                        }
                )
                .filter(r -> !r.isDeleted()).map(Command.Result::result);
    }

    /**
     * Performs a recursive calculation by applying the stream's command to the given value, ensuring that all commands
     * in the processing chain are executed in sequence. This method is a key component of the stream's functionality,
     * enabling the transformation or computation of stream elements through a series of commands.
     * <p>
     * When invoked, this method checks if there is a downstream {@code ThreadedStream}. If there isn't one, indicating
     * that this instance is at the end of the command chain, it simply wraps the input value in a {@link Command.Result}
     * and returns it. However, if a downstream exists, the method first recursively calls {@code calculate()} on the downstream
     * stream, passing the input value for processing. Once the downstream processing is completed, and if the result is not
     * marked as deleted, it applies the current stream's command to the downstream's result. This recursive approach ensures
     * that all commands in the chain are executed, starting from the terminal command back to the initial command.
     * <p>
     * The method also handles thread interruptions by immediately returning a deleted result if the current thread is
     * interrupted. This ensures the stream's ability to deal with interruptions in a multithreaded environment, maintaining
     * the correct processing semantics.
     *
     * @param value the input value to be processed by this and possibly downstream streams' commands.
     * @return a {@link Command.Result} object encapsulating the result of the computation through the command chain.
     * If the computation is interrupted or deemed invalid, a deleted result is returned to signify this state.
     * @throws ClassCastException if the command's execution type does not match the provided value's type.
     */
    Command.Result<T> calculate(Object value) {
        if (Thread.interrupted()) {
            return deleted();
        }
        if (downstream == null) {
            //noinspection unchecked
            return new Command.Result<>((T) value);
        } else {
            try {
                final var result = downstream.calculate(value);
                if (result.isDeleted() || result.exception() != null) {
                    //noinspection unchecked
                    return (Command.Result<T>) result;
                }
                //noinspection unchecked
                return (Command.Result<T>) command.execute((T) result.result());
            } catch (Exception e) {
                return exception(e);
            }
        }
    }

    @Override
    public Stream<T> filter(Predicate<? super T> predicate) {
        //noinspection unchecked
        return filteredStream(new Command.Filter<>((Predicate<T>) predicate));
    }

    private ThreadedStream<T> filteredStream(Command<T, T> command) {
        return new ThreadedStream<>(command, this, source);
    }


    @Override
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
        limit = maxSize;
        return this;
    }


    @Override
    public Stream<T> skip(long n) {
        if (source.isParallel()) {
            return filteredStream(new Command.Skip<>(n));
        } else {
            return toStream().skip(n);
        }
    }


    @Override
    public void forEach(Consumer<? super T> action) {
        toStream().forEach(action);
    }


    @Override
    public void forEachOrdered(Consumer<? super T> action) {
        toOrderedStream().forEachOrdered(action);
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
        if (isParallel()) {
            return filteredStream(new Command.FindFirst<>()).toStream().findFirst();
        } else {
            return toStream().findFirst();
        }
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
        return source.isParallel();
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
