package javax0.vtstream;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.*;
import java.util.stream.*;

import static javax0.vtstream.Command.deleted;
import static javax0.vtstream.Command.exception;

@SuppressWarnings("NullableProblems")
public class ThreadedStream<T> implements Stream<T> {

    /**
     * The command to be applied to each element of the downstream or the source if there is no downstream,
     * producing elements of the constructed stream.
     * <p>
     * It is not possible to represent the type of the command's input parameter and that of the downstream
     * without introducing another type argument, and complicating the interface; however, the constructors
     * ensure that the command is compatible with the downstream.
     */
    private final Command<Object, T> command;
    /**
     * The downstream stream from which elements are processed by the command.
     */
    private final ThreadedStream<?> downstream;

    /**
     * The source stream from which elements are processed by the command.
     * If there is a downstream, the source stream is inherited from the downstream.
     */
    private final Stream<?> source;

    /**
     * The maximum number of elements to be processed by the stream. If set to a positive value, the stream
     * will process at most this number of elements. If set to a negative value, the stream will process all
     * elements from the source stream.
     */
    private long limit = -1;

    /**
     * Indicates whether the stream has been operated upon. If true, the stream is considered
     * consumed and no further operations can be chained to it.
     * <p>
     * This is a simplified version of the original Stream API's implementation, which uses a
     * more complex mechanism to track the state of the stream. The original implementation
     * uses a {@code StreamShape} object to track the state of the stream, which is updated
     * when the stream is operated upon. This mechanism is not used here, as it is not necessary
     * for the purposes of this code.
     */
    private boolean chained = false;
    private static final String MSG_STREAM_LINKED = "stream has already been operated upon or closed";

    /**
     * Constructs a new ThreadedStream instance by applying a command on a downstream
     *
     * @param command    the command to be applied to each element of the downstream, producing elements of the constructed stream
     * @param downstream the stream from where we get the elements to be processed by the command
     * @param <S>        the type of the source (downstream)
     */
    private <S> ThreadedStream(Command<? super S, ? extends T> command, ThreadedStream<S> downstream) {
        this(command, downstream, () -> {
        });
    }

    /**
     * Constructs a new ThreadedStream instance by applying a command on a downstream
     *
     * @param command      the command to be applied to each element of the downstream, producing elements of the constructed stream
     * @param downstream   the stream from where we get the elements to be processed by the command
     * @param closeHandler a Runnable to be executed when the stream is closed
     * @param <S>          the type of the source (downstream)
     */
    private <S> ThreadedStream(Command<? super S, ? extends T> command, ThreadedStream<S> downstream, Runnable closeHandler) {
        // we'll lose '<S>' after this point, but we know the command is compatible with the downstream;
        // cast the command now, so we don't need to cast it later:
        // its input will come from downstream, so will be type S, which will be erased into Object anyway
        // we don't care what exact subtype of T the result is
        //noinspection unchecked
        this.command = (Command<Object, T>) command;
        this.downstream = Objects.requireNonNull(downstream, "downstream cannot ne null");
        if (downstream.chained) {
            throw new IllegalStateException(MSG_STREAM_LINKED);
        }
        downstream.chained = true;
        this.source = downstream.source;
        this.limit = downstream.limit;
        this.closeHandler = closeHandler;
    }

    private ThreadedStream(Stream<T> source) {
        this.command = null;
        this.downstream = null;
        this.source = source;
        this.closeHandler = () -> {
        };
    }

    public static <K> ThreadedStream<K> threaded(Stream<K> source) {
        return new ThreadedStream<>(source);
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
        } finally {
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
        class Task {
            Thread workerThread;
            volatile Command.Result<T> result;

            /**
             * Wait for the thread calculating the result of the task to be finished. This method is blocking.
             * @param task the task to wait for
             */
            static void waitForResult(Task task) {
                try {
                    task.workerThread.join();
                } catch (InterruptedException e) {
                    task.result = deleted();
                }
            }
        }
        final var tasks = Collections.synchronizedList(new LinkedList<Task>());

        final Stream<?> limitedSource = limit >= 0 ? source.limit(limit) : source;
        limitedSource.forEach(
                sourceItem -> {
                    Task task = new Task();
                    tasks.add(task);
                    task.workerThread = Thread.startVirtualThread(() -> task.result = calculate(sourceItem));
                }
        );

        return tasks.stream()
                .peek(Task::waitForResult)
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
                Command.Result<?> downstreamResult = downstream.calculate(value);
                if (downstreamResult.isDeleted() || downstreamResult.exception() != null) {
                    //noinspection unchecked
                    return (Command.Result<T>) downstreamResult;
                }
                //noinspection unchecked
                return command == null ? (Command.Result<T>) downstreamResult.result() : command.execute(downstreamResult.result());
            } catch (Exception e) {
                return exception(e);
            }
        }
    }

    @Override
    public Stream<T> filter(Predicate<? super T> predicate) {
        return filteredStream(new Command.Filter<>(predicate));
    }

    private ThreadedStream<T> filteredStream(Command<T, T> command) {
        return new ThreadedStream<>(command, this);
    }


    @Override
    public <R> ThreadedStream<R> map(Function<? super T, ? extends R> mapper) {
        return new ThreadedStream<>(new Command.Map<>(mapper), this);
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
        return new ThreadedStream<>(new Command.NoOp<>(), this, closeHandler);
    }


    @Override
    public void close() {
        if (downstream != null) {
            downstream.close();
        }
        closeHandler.run();
    }
}
