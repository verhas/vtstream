package javax0.vtstream;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.*;

import static org.junit.jupiter.api.Assertions.*;

public class TestThreadedStream {

    @Test
    @DisplayName("Simple parallel execution")
    void testParallelExecution() {
        AtomicInteger i = new AtomicInteger(30);
        final var s = ThreadedStream.threaded(Stream.of("a", "b", "c").parallel()).map(k ->
                {
                    try {
                        Thread.sleep(i.getAndAdd(-10));
                    } catch (InterruptedException ignore) {
                    }
                    return k + k;
                }
        ).collect(Collectors.toSet());
        assertEquals(Set.of("aa", "bb", "cc"), s);
    }

    @Test
    @DisplayName("Simple parallel execution using the ForJoinPool common pool")
    void testParallelExecutionUsingNormalThreads() {
        AtomicInteger i = new AtomicInteger(30);
        final var s = ThreadedStream.threaded(Stream.of("a", "b", "c").parallel(), ForkJoinPool.commonPool()).map(k ->
                {
                    try {
                        Thread.sleep(i.getAndAdd(-10));
                    } catch (InterruptedException ignore) {
                    }
                    return k + k;
                }
        ).collect(Collectors.toSet());
        assertEquals(Set.of("aa", "bb", "cc"), s);
    }

    @Test
    @DisplayName("Exception in thread")
    void testExceptionInThread() {
        Assertions.assertThrows(RuntimeException.class, () -> ThreadedStream.threaded(Stream.of("a", "b", "c")).map(k ->
                {
                    throw new RuntimeException("Test exception");
                }
        ).collect(Collectors.toSet()));
    }

    @Test
    @DisplayName("Exception in thread unordered")
    void testExceptionInThreadUnordered() {
        Assertions.assertThrows(RuntimeException.class, () -> ThreadedStream.threaded(Stream.of("a", "b", "c").parallel()).map(k ->
                {
                    throw new RuntimeException("Test exception");
                }
        ).collect(Collectors.toSet()));
    }

    @Test
    @DisplayName("Simple serial execution")
    void testSerialExecution() {
        AtomicInteger i = new AtomicInteger(30);
        final var s = ThreadedStream.threaded(Stream.of("a", "b", "c")).map(k ->
                {
                    try {
                        Thread.sleep(i.getAndAdd(-10));
                    } catch (InterruptedException ignore) {
                    }
                    return k + k;
                }
        ).toList();
        assertEquals(List.of("aa", "bb", "cc"), s);
    }

    @Test
    @DisplayName("Test filtering")
    void testFilterExecution() {
        final var s = ThreadedStream.threaded(Stream.of("a", "b", "c").parallel())
                .map(k -> k + k)
                .filter(k -> k.startsWith("a"))
                .findAny();
        assertTrue(s.isPresent());
        assertEquals("aa", s.get());
    }

    @Test
    public void testFilter() {
        Stream<String> sourceStream = Stream.of("apple", "banana", "cherry", "apricot");
        ThreadedStream<String> threadedStream = ThreadedStream.threaded(sourceStream);
        List<String> result = threadedStream.filter(s -> s.startsWith("a")).toList();
        assertEquals(2, result.size(), "Filter does not work correctly, size is " + result.size() + " instead of 2");
        assertTrue(result.contains("apple") && result.contains("apricot"), "Filter does not work correctly, does not contain the correct elements");
    }

    @Test
    public void testMap() {
        Stream<Integer> sourceStream = Stream.of(1, 2, 3, 4);
        ThreadedStream<Integer> threadedStream = ThreadedStream.threaded(sourceStream);
        List<String> result = threadedStream.map(i -> "" + 2*i).toList();
        assertArrayEquals(new String[]{"2", "4", "6", "8"}, result.toArray(), "Map does not work correctly");
    }

    @Test
    public void testMapIntegerToString() {
        Stream<Integer> sourceStream = Stream.of(1, 2, 3, 4);
        ThreadedStream<Integer> threadedStream = ThreadedStream.threaded(sourceStream);
        List<String> result = threadedStream.map(i -> "" + i).toList();
        assertArrayEquals(new String[]{"1", "2", "3", "4"}, result.toArray(), "Map does not work correctly");
    }

    @Test
    public void testCount() {
        Stream<String> sourceStream = Stream.of("apple", "banana", "cherry");
        ThreadedStream<String> threadedStream = ThreadedStream.threaded(sourceStream);
        long count = threadedStream.count();
        assertEquals(3, count, "Count does not return the correct number of elements");
    }

    @Test
    public void testDistinct() {
        Stream<String> sourceStream = Stream.of("apple ", "banana", "apple ", "cherry", "banana");
        ThreadedStream<String> threadedStream = ThreadedStream.threaded(sourceStream);
        List<String> result = threadedStream.distinct().toList();
        assertEquals(3, result.size(), "Distinct does not remove duplicates correctly result is " + result);
    }

    @Test
    public void testMapToIntOperation() {
        // Given a source stream of integers
        Stream<Integer> source = Stream.of(1, 2, 3, 4, 5);
        ThreadedStream<Integer> threadedStream = ThreadedStream.threaded(source);

        // When we apply a mapToInt operation to double each number
        IntStream resultStream = threadedStream.mapToInt(x -> x * 2);

        // Then we expect to get an IntStream with each element doubled
        int[] expectedResults = {2, 4, 6, 8, 10};
        assertArrayEquals(expectedResults, resultStream.toArray());
    }

    @Test
    public void testMapToLongOperation() {
        // Given a source stream of integers
        Stream<Integer> source = Stream.of(1, 2, 3, 4, 5);
        ThreadedStream<Integer> threadedStream = ThreadedStream.threaded(source);

        // When we apply a mapToLong operation to triple each number
        LongStream resultStream = threadedStream.mapToLong(x -> x * 3L);

        // Then we expect to get a LongStream with each element tripled
        long[] expectedResults = {3L, 6L, 9L, 12L, 15L};
        assertArrayEquals(expectedResults, resultStream.toArray());
    }

    @Test
    public void testMapToDoubleOperation() {
        // Given a source stream of integers
        Stream<Integer> source = Stream.of(1, 2, 3, 4, 5);
        ThreadedStream<Integer> threadedStream = ThreadedStream.threaded(source);

        // When we apply a mapToDouble operation to square each number and convert it to double
        DoubleStream resultStream = threadedStream.mapToDouble(x -> x * x);

        // Then we expect to get a DoubleStream with each element squared
        double[] expectedResults = {1.0, 4.0, 9.0, 16.0, 25.0};
        assertArrayEquals(expectedResults, resultStream.toArray(), 0.01);
    }

    @Test
    public void testFlatMapToIntOperation() {
        Stream<Integer> source = Stream.of(1, 2, 3);
        ThreadedStream<Integer> threadedStream = ThreadedStream.threaded(source);

        // Use flatMapToInt to convert each integer into a range of ints starting from 0 to the integer itself
        IntStream resultStream = threadedStream.flatMapToInt(x -> IntStream.range(0, x));

        // Expecting to get a flattened stream of ints
        int[] expectedResults = {0, 0, 1, 0, 1, 2};
        assertArrayEquals(expectedResults, resultStream.toArray());
    }

    @Test
    public void testFlatMapToLongOperation() {
        Stream<Integer> source = Stream.of(1, 2, 3);
        ThreadedStream<Integer> threadedStream = ThreadedStream.threaded(source);

        // Use flatMapToLong to convert each integer into a range of longs, demonstrating a simple mapping
        LongStream resultStream = threadedStream.flatMapToLong(x -> LongStream.range(0, x));

        // Expecting to get a flattened stream of longs
        long[] expectedResults = {0L, 0L, 1L, 0L, 1L, 2L};
        assertArrayEquals(expectedResults, resultStream.toArray());
    }

    @Test
    public void testFlatMapToDoubleOperation() {
        Stream<Integer> source = Stream.of(1, 2, 3);
        ThreadedStream<Integer> threadedStream = ThreadedStream.threaded(source);

        // Use flatMapToDouble to map each integer to a stream of doubles representing its division by 2.0, for two steps
        DoubleStream resultStream = threadedStream.flatMapToDouble(x -> DoubleStream.of(x / 2.0, x / 2.0 + 0.5));

        // Expecting to get a flattened stream of doubles
        double[] expectedResults = {0.5, 1.0, 1.0, 1.5, 1.5, 2.0};
        assertArrayEquals(expectedResults, resultStream.toArray(), 0.01);
    }

    @Test
    public void testFlatMapOperation() {
        // Given a source stream of lists
        Stream<List<Integer>> source = Stream.of(
                Arrays.asList(1, 2),
                Arrays.asList(3, 4),
                Arrays.asList(5, 6)
        );
        ThreadedStream<List<Integer>> threadedStream = ThreadedStream.threaded(source);

        // When we apply a flatMap operation to convert each list into a stream of integers
        Stream<Integer> resultStream = threadedStream.flatMap(List::stream);

        // Then we expect to get a flattened stream of integers
        List<Integer> expectedResult = Arrays.asList(1, 2, 3, 4, 5, 6);
        List<Integer> actualResult = resultStream.collect(Collectors.toList());

        assertIterableEquals(expectedResult, actualResult);
    }

    @Test
    public void testSortedOperation() {
        Stream<Integer> source = Stream.of(3, 1, 4, 1, 5, 9);
        ThreadedStream<Integer> threadedStream = ThreadedStream.threaded(source);

        List<Integer> sortedList = threadedStream.sorted().collect(Collectors.toList());

        assertEquals(Arrays.asList(1, 1, 3, 4, 5, 9), sortedList);
    }

    @Test
    public void testPeekOperation() {
        List<Integer> sourceList = Arrays.asList(1, 2, 3, 4, 5);
        List<Integer> resultList = Collections.synchronizedList(new ArrayList<>());
        ThreadedStream<Integer> threadedStream = ThreadedStream.threaded(sourceList.stream());

        final var result = threadedStream.peek(resultList::add).toList(); // Consuming stream to trigger peek

        assertEquals(new HashSet<>(sourceList), new HashSet<>(resultList));
        assertEquals(new HashSet<>(sourceList), new HashSet<>(result));
    }

    @Test
    public void testLimitOperation() {
        Stream<Integer> source = Stream.iterate(0, n -> n + 1);
        ThreadedStream<Integer> threadedStream = ThreadedStream.threaded(source);

        List<Integer> limitedList = threadedStream.limit(5).collect(Collectors.toList());

        assertEquals(Arrays.asList(0, 1, 2, 3, 4), limitedList);
    }

    @Test
    public void testSkipOperation() {
        Stream<Integer> source = Stream.of(1, 2, 3, 4, 5);
        ThreadedStream<Integer> threadedStream = ThreadedStream.threaded(source);

        List<Integer> skippedList = threadedStream.skip(2).collect(Collectors.toList());

        assertEquals(Arrays.asList(3, 4, 5), skippedList);
    }

    @Test
    public void testSkipOperationParallel() {
        Stream<Integer> source = Stream.of(1, 2, 3, 4, 5).parallel();
        ThreadedStream<Integer> threadedStream = ThreadedStream.threaded(source);

        List<Integer> skippedList = threadedStream.skip(2).toList();

        assertEquals(3, skippedList.size());
    }

    @Test
    public void testSorted() {
        ThreadedStream<Integer> threadedStream = ThreadedStream.threaded(Stream.of(3, 1, 4, 1, 5, 9));
        List<Integer> sortedList = threadedStream.sorted().collect(Collectors.toList());
        assertEquals(Arrays.asList(1, 1, 3, 4, 5, 9), sortedList);
    }

    @Test
    public void testPeek() {
        List<Integer> peekedElements = new CopyOnWriteArrayList<>();
        ThreadedStream<Integer> threadedStream = ThreadedStream.threaded(Stream.of(1, 2, 3, 4, 5));
        List<Integer> resultList = threadedStream.peek(peekedElements::add).collect(Collectors.toList());

        assertEquals(Arrays.asList(1, 2, 3, 4, 5), resultList);
        assertTrue(peekedElements.containsAll(Arrays.asList(1, 2, 3, 4, 5)), "Peeked elements do not match expected values.");
    }

    @Test
    public void testForEach() {
        List<Integer> consumedElements = new CopyOnWriteArrayList<>();
        ThreadedStream<Integer> threadedStream = ThreadedStream.threaded(Stream.of(1, 2, 3, 4, 5));
        threadedStream.forEach(consumedElements::add);

        assertTrue(consumedElements.containsAll(Arrays.asList(1, 2, 3, 4, 5)), "forEach did not consume all elements.");
    }

    @Test
    public void testForEachOrdered() {
        List<Integer> consumedElements = Collections.synchronizedList(new ArrayList<>());
        ThreadedStream<Integer> threadedStream = ThreadedStream.threaded(Stream.of(5, 4, 3, 2, 1));
        threadedStream.forEachOrdered(consumedElements::add);

        assertEquals(Arrays.asList(5, 4, 3, 2, 1), consumedElements, "forEachOrdered did not maintain order.");
    }

    @Test
    public void testToArrayWithGenerator() {
        ThreadedStream<String> threadedStream = ThreadedStream.threaded(Stream.of("one", "two", "three"));
        String[] resultArray = threadedStream.toArray(String[]::new);

        assertArrayEquals(new String[]{"one", "two", "three"}, resultArray, "The toArray method did not produce the expected array.");
    }

    @Test
    public void testReduceWithIdentityAndAccumulator() {
        ThreadedStream<Integer> threadedStream = ThreadedStream.threaded(Stream.of(1, 2, 3, 4, 5));
        Integer sum = threadedStream.reduce(0, Integer::sum);

        assertEquals(15, sum, "The reduce method did not compute the sum correctly.");
    }

    @Test
    public void testReduceWithAccumulator() {
        ThreadedStream<Integer> threadedStream = ThreadedStream.threaded(Stream.of(1, 2, 3, 4, 5));
        Optional<Integer> sum = threadedStream.reduce(Integer::sum);

        assertTrue(sum.isPresent(), "The reduce method did not produce a result.");
        assertEquals(15, sum.get(), "The reduce method did not compute the sum correctly.");
    }

    @Test
    public void testReduceWithIdentityAccumulatorAndCombiner() {
        ThreadedStream<Integer> threadedStream = ThreadedStream.threaded(Stream.of(1, 2, 3, 4, 5));
        Integer product = threadedStream.reduce(1, (acc, x) -> acc * x, (left, right) -> left * right);

        assertEquals(120, product, "The reduce method did not compute the product correctly.");
    }

    @Test
    public void testCollect() {
        ThreadedStream<String> threadedStream = ThreadedStream.threaded(Stream.of("apple", "banana", "cherry"));
        List<String> resultList = threadedStream.collect(Collectors.toList());

        assertIterableEquals(List.of("apple", "banana", "cherry"), resultList, "The collect method did not produce the expected list.");
    }

    @Test
    public void testMin() {
        ThreadedStream<Integer> threadedStream = ThreadedStream.threaded(Stream.of(5, 3, 8, 2, 9));
        Optional<Integer> min = threadedStream.min(Integer::compare);

        assertTrue(min.isPresent(), "The min method did not find a minimum value.");
        assertEquals(2, min.get(), "The min method did not find the correct minimum value.");
    }

    @Test
    public void testMax() {
        ThreadedStream<Integer> threadedStream = ThreadedStream.threaded(Stream.of(5, 3, 8, 2, 9));
        Optional<Integer> max = threadedStream.max(Integer::compare);

        assertTrue(max.isPresent(), "The max method did not find a maximum value.");
        assertEquals(9, max.get(), "The max method did not find the correct maximum value.");
    }


    @Test
    public void testAnyMatch() {
        ThreadedStream<Integer> threadedStream = ThreadedStream.threaded(Stream.of(1, 2, 3, 4, 5));
        boolean hasEven = threadedStream.anyMatch(x -> x % 2 == 0);

        assertTrue(hasEven, "The anyMatch method failed to identify an even number.");
    }

    @Test
    public void testNoneMatch() {
        ThreadedStream<Integer> threadedStream = ThreadedStream.threaded(Stream.of(1, 3, 5));
        boolean noEven = threadedStream.noneMatch(x -> x % 2 == 0);

        assertTrue(noEven, "The noneMatch method incorrectly identified an even number.");
    }

    @Test
    public void testFindFirst() {
        ThreadedStream<Integer> threadedStream = ThreadedStream.threaded(Stream.of(4, 2, 6, 8, 3));
        Optional<Integer> first = threadedStream.findFirst();

        assertTrue(first.isPresent(), "The findFirst method did not find any element.");
        assertEquals(4, first.get(), "The findFirst method did not return the first element.");
    }

    @Test
    public void testFindAny() {
        ThreadedStream<Integer> threadedStream = ThreadedStream.threaded(Stream.of(1, 2, 3, 4, 5));
        Optional<Integer> any = threadedStream.findAny();

        assertTrue(any.isPresent(), "The findAny method did not find any element.");
    }

    @Test
    public void testIterator() {
        ThreadedStream<Integer> threadedStream = ThreadedStream.threaded(Stream.of(1, 2, 3));
        Iterator<Integer> iterator = threadedStream.iterator();

        List<Integer> result = new ArrayList<>();
        iterator.forEachRemaining(result::add);

        assertEquals(Arrays.asList(1, 2, 3), result, "The iterator did not iterate over the stream elements correctly.");
    }

    @Test
    public void testSpliterator() {
        ThreadedStream<Integer> threadedStream = ThreadedStream.threaded(Stream.of(1, 2, 3));
        Spliterator<Integer> spliterator = threadedStream.spliterator();

        assertTrue(spliterator.estimateSize() > 0, "The original spliterator did not have elements.");
    }

    @Test
    public void testIsParallel() {
        ThreadedStream<Integer> threadedStream = ThreadedStream.threaded(Stream.of(1, 2, 3).parallel());
        assertTrue(threadedStream.isParallel(), "The stream should be parallel.");
    }

    @Test
    public void testSequential() {
        final var threadedStream = ThreadedStream.threaded(Stream.of(1, 2, 3).parallel()).sequential();
        assertFalse(threadedStream.isParallel(), "The stream should be sequential after calling sequential().");
    }

    @Test
    public void testParallel() {
        final var threadedStream = ThreadedStream.threaded(Stream.of(1, 2, 3)).parallel();
        assertTrue(threadedStream.isParallel(), "The stream should be parallel after calling parallel().");
    }

    @Test
    public void testUnorderedStreamBehavior() {
        ThreadedStream<Integer> orderedStream = ThreadedStream.threaded(Stream.of(1, 2, 3, 4, 5, 6, 7, 8, 9, 10));

        // Applying unordered to the stream
        Stream<Integer> unorderedStream = orderedStream.unordered();

        // Perform an operation that works differently on ordered vs unordered streams
        // Here, we use limit() which does not guarantee which elements are returned in an unordered stream
        List<Integer> results = unorderedStream.limit(5).toList();

        // Since we cannot assert specific elements due to unordered nature, we check for size and non-null elements
        assertEquals(5, results.size(), "The unordered stream should limit the results to 5 elements.");
        assertTrue(results.stream().noneMatch(Objects::isNull), "The unordered stream should not contain null elements.");

        // Verify that attempting to use the original ordered stream after unordered() does not cause exceptions
        // Note: This specific test scenario might need to be adjusted based on how ThreadedStream is implemented
        // and how it handles stream state and closure.
        assertDoesNotThrow(orderedStream::close, "Closing the ordered stream after unordered() should not throw an exception.");
    }

    @Test
    public void testOnClose() {
        final AtomicBoolean closed = new AtomicBoolean(false); // Use array to modify inside lambda
        final var threadedStream = ThreadedStream.threaded(Stream.of("apple", "banana", "cherry"))
                .onClose(() -> closed.set(true));

        assertFalse(closed.get(), "The close handler should not have been called yet.");
        //noinspection ResultOfMethodCallIgnored
        threadedStream.toList();// This should trigger the close handler
        assertTrue(closed.get(), "The close handler should have been called after closing the stream.");
    }

    @Test
    public void testClose() {
        final boolean[] closed = {false};
        final var threadedStream = ThreadedStream.threaded(Stream.of("apple", "banana", "cherry"))
                .onClose(() -> closed[0] = true);

        threadedStream.close(); // This should trigger the close handler
        assertTrue(closed[0], "The close handler should have been executed upon closing the stream.");
    }

    @Test
    public void testIllegalChainingOfThreadedStream() {
        // Create the initial downstream ThreadedStream
        final var k = ThreadedStream.threaded(Stream.of(1, 2, 3));
        final var t = k.map(x -> x * 2);
        //noinspection DataFlowIssue
        Assertions.assertThrows(IllegalStateException.class, () -> k.map(x -> x * 2),
                "Expected an IllegalStateException to be thrown when chaining a ThreadedStream into two different directions");
    }

}
