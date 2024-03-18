package javax0.vtstream;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;

/**
 * Represents an abstract command to be performed on elements of a stream.
 * This class and its subclasses encapsulate operations such as mapping, filtering,
 * distinct, and others that can be applied to stream elements.
 * <p>
 * The {@code Command} class uses a generic type {@code <T>} for input elements and {@code <R>}
 * for the result of the command execution. Each command processes elements of type {@code T}
 * and produces results of type {@code R}. Commands can modify elements, filter them, or apply
 * any other operation as defined in their specific implementation.
 *
 * @param <T> the type of the input elements to the command
 * @param <R> the type of the result of the command execution
 */
abstract class Command<T, R> {

    /**
     * A record that encapsulates the result of executing a command. It can represent
     * a valid result or a special marker indicating that an element has been "deleted"
     * or filtered out by the command.
     *
     * @param <R> the type of the result
     */
    public record Result<R>(R result, Exception exception) {
        /**
         * A static final instance of Result representing a deleted or filtered out element.
         */
        static final Result<Object> DELETED = new Result<>(null, null);

        /**
         * Checks if this result represents a deleted element.
         *
         * @return {@code true} if this result is the special DELETED marker, {@code false} otherwise.
         */
        public boolean isDeleted() {
            return this == DELETED;
        }

        public Result(R result) {
            this(result, null);
        }

        public Result(Exception exception) {
            this(null, exception);
        }
    }

    /**
     * Creates a special {@code Result} instance representing a deleted or filtered out element.
     *
     * @param <T> the type of the input elements
     * @return a {@code Result} instance representing a deleted element.
     */
    public static <T> Result<T> deleted() {
        //noinspection unchecked
        return (Result<T>) Result.DELETED;
    }

    public static <T> Result<T> exception(Exception e) {
        //noinspection unchecked
        return new Result<>(e);
    }

    /**
     * Executes the command on a given element.
     *
     * @param t the input element to process
     * @return the result of executing the command on the input element
     */
    public abstract Result<R> execute(T t);

    /**
     * Helper method to create a result unless a condition is met, in which case it returns
     * a "deleted" result.
     *
     * @param isDeleted the condition that determines if the result should be "deleted"
     * @param t         the input element to wrap in a {@code Result}, unless it is "deleted"
     * @return a {@code Result} wrapping the input element or a "deleted" result
     */
    private static <T> Result<T> unless(boolean isDeleted, T t) {
        return isDeleted ? deleted() : new Result<>(t);
    }

    public static class Filter<T> extends Command<T, T> {
        private final Predicate<? super T> predicate;

        public Filter(Predicate<? super T> predicate) {
            this.predicate = predicate;
        }

        @Override
        public Result<T> execute(T t) {
            return unless(!predicate.test(t), t);
        }
    }

    /**
     * The command Any match is invoked as a last stage from the terminal operation anyMatch.
     * <p>
     * This command deletes all entries that do not match and also those that come after the first match. There remains
     * one undeleted entry, a boolean true. The actual value is not important. This operation optimizes the execution of
     * the thread so that the elements that are not needed may not be executed.
     *
     * @param <T>
     */
    public static class AnyMatch<T> extends Command<T, T> {
        private final Predicate<? super T> predicate;
        private final AtomicBoolean match = new AtomicBoolean(false);

        public AnyMatch(Predicate<? super T> predicate) {
            this.predicate = predicate;
        }

        @Override
        public synchronized Result<T> execute(T t) {
            if (!match.get() && predicate.test(t)) {
                try {
                    return new Result<>(t);
                } finally {
                    match.set(true);
                }
            }
            return deleted();
        }
    }

    public static class FindFirst<T> extends Command<T, T> {
        private final AtomicBoolean match = new AtomicBoolean(false);

        @Override
        public Result<T> execute(T t) {
            return unless(match.getAndSet(true), t);
        }
    }

    public static class FindAny<T> extends Command<T, T> {
        private final AtomicBoolean match = new AtomicBoolean(false);

        @Override
        public Result<T> execute(T t) {
            return unless(match.getAndSet(true), t);
        }

    }

    public static class NoOp<T> extends Command<T, T> {

        @Override
        public Result<T> execute(T t) {
            return new Result<>(t);
        }
    }

    public static class Distinct<T> extends Command<T, T> {
        private final Set<T> accumulator = new HashSet<>();

        @Override
        public Result<T> execute(T t) {
            synchronized (this) {
                try {
                    return unless(accumulator.contains(t), t);
                } finally {
                    accumulator.add(t);
                }
            }
        }
    }

    public static class Skip<T> extends Command<T, T> {
        private final AtomicLong n;

        public Skip(long n) {
            this.n = new AtomicLong(n);
        }

        @Override
        public Result<T> execute(T t) {
            return unless(n.getAndDecrement() > 0, t);
        }
    }

    public static class Peek<T> extends Command<T, T> {
        private final Consumer<? super T> action;

        public Peek(Consumer<? super T> action) {
            this.action = action;
        }

        @Override
        public Result<T> execute(T t) {
            action.accept(t);
            return new Result<>(t);
        }
    }

    public static class Map<T, R> extends Command<T, R> {
        private final Function<T, R> transform;

        public Map(Function<T, R> transform) {
            this.transform = transform;
        }

        @Override
        public Result<R> execute(T t) {
            return new Result<>(transform.apply(t));
        }

    }

}
