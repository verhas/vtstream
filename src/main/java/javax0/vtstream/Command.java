package javax0.vtstream;

import java.util.function.Function;
import java.util.function.Predicate;

/**
 * Performs a stream command, like Map, Filter and so on.
 *
 * @param <T>
 * @param <R>
 */
abstract class Command<T, R> {
    static class Result<R> {
        final boolean isDeleted;
        final R result;

        Result(boolean isDeleted, R result) {
            this.isDeleted = isDeleted;
            this.result = result;
        }
    }

    public abstract Result<R> execute(T t);

    public static class Filter<T> extends Command<T, T> {
        private final Predicate<T> predicate;

        public Filter(Predicate<T> predicate) {
            this.predicate = predicate;
        }

        @Override
        public Result<T> execute(T t) {
            return new Result<T>(predicate.test(t), t);
        }
    }

    public static class Map<T, R> extends Command<T, R> {
        private final Function<T, R> transform;

        public Map(Function<T, R> transform) {
            this.transform = transform;
        }

        @Override
        public Result<R> execute(T t) {
            return new Result<R>(false, transform.apply(t));
        }
    }

}
