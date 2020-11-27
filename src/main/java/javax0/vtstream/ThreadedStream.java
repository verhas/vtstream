package javax0.vtstream;

import java.util.Comparator;
import java.util.Iterator;
import java.util.Optional;
import java.util.Spliterator;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.BinaryOperator;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.IntFunction;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collector;

public class ThreadedStream<T> {

    private final Command<T,?> command;
private final ThreadedStream<?> downstream;

    public ThreadedStream(Command<T, ?> command, ThreadedStream<?> downstream) {
        this.command = command;
        this.downstream = downstream;
    }

    public ThreadedStream<T> filter(Predicate<? super T> predicate) {
        return new ThreadedStream<T>(new Command.Filter<T>((Predicate<T>) predicate), this);
    }


    public <R> ThreadedStream<R> map(Function<? super T, ? extends R> mapper) {
        return new ThreadedStream<R>((Command<R, ?>) new Command.Map<T,R>((Function<T, R>) mapper), this);
    }

/* TODO
    public IntThreadedStream mapToInt(ToIntFunction<? super T> mapper) {
        throw new RuntimeException("not implemented");
    }


    public LongThreadedStream mapToLong(ToLongFunction<? super T> mapper) {
        throw new RuntimeException("not implemented");
    }


    public DoubleThreadedStream mapToDouble(ToDoubleFunction<? super T> mapper) {
        throw new RuntimeException("not implemented");
    }


    public <R> ThreadedStream<R> flatMap(Function<? super T, ? extends ThreadedStream<? extends R>> mapper) {
        throw new RuntimeException("not implemented");
    }


    public IntThreadedStream flatMapToInt(Function<? super T, ? extends IntThreadedStream> mapper) {
        throw new RuntimeException("not implemented");
    }


    public LongThreadedStream flatMapToLong(Function<? super T, ? extends LongThreadedStream> mapper) {
        throw new RuntimeException("not implemented");
    }


    public DoubleThreadedStream flatMapToDouble(Function<? super T, ? extends DoubleThreadedStream> mapper) {
        throw new RuntimeException("not implemented");
    }
*/

    public ThreadedStream<T> distinct() {
        throw new RuntimeException("not implemented");
    }


    public ThreadedStream<T> sorted() {
        throw new RuntimeException("not implemented");
    }


    public ThreadedStream<T> sorted(Comparator<? super T> comparator) {
        throw new RuntimeException("not implemented");
    }


    public ThreadedStream<T> peek(Consumer<? super T> action) {
        throw new RuntimeException("not implemented");
    }


    public ThreadedStream<T> limit(long maxSize) {
        throw new RuntimeException("not implemented");
    }


    public ThreadedStream<T> skip(long n) {
        throw new RuntimeException("not implemented");
    }


    public void forEach(Consumer<? super T> action) {

    }


    public void forEachOrdered(Consumer<? super T> action) {

    }


    public Object[] toArray() {
        return new Object[0];
    }


    public <A> A[] toArray(IntFunction<A[]> generator) {
        throw new RuntimeException("not implemented");
    }


    public T reduce(T identity, BinaryOperator<T> accumulator) {
        throw new RuntimeException("not implemented");
    }


    public Optional<T> reduce(BinaryOperator<T> accumulator) {
        return Optional.empty();
    }


    public <U> U reduce(U identity, BiFunction<U, ? super T, U> accumulator, BinaryOperator<U> combiner) {
        throw new RuntimeException("not implemented");
    }


    public <R> R collect(Supplier<R> supplier, BiConsumer<R, ? super T> accumulator, BiConsumer<R, R> combiner) {
        throw new RuntimeException("not implemented");
    }


    public <R, A> T collect(Collector<? super R, A, T> collector) {
        return new ThreadedCollector<>(collector, this).getResult();
    }


    public Optional<T> min(Comparator<? super T> comparator) {
        return Optional.empty();
    }


    public Optional<T> max(Comparator<? super T> comparator) {
        return Optional.empty();
    }


    public long count() {
        return 0;
    }


    public boolean anyMatch(Predicate<? super T> predicate) {
        return false;
    }


    public boolean allMatch(Predicate<? super T> predicate) {
        return false;
    }


    public boolean noneMatch(Predicate<? super T> predicate) {
        return false;
    }


    public Optional<T> findFirst() {
        return Optional.empty();
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
        return false;
    }


    public ThreadedStream<T> sequential() {
        throw new RuntimeException("not implemented");
    }


    public ThreadedStream<T> parallel() {
        throw new RuntimeException("not implemented");
    }


    public ThreadedStream<T> unordered() {
        throw new RuntimeException("not implemented");
    }


    public ThreadedStream<T> onClose(Runnable closeHandler) {
        throw new RuntimeException("not implemented");
    }


    public void close() {

    }
}
