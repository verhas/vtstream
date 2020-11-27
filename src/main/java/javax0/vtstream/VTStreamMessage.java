package javax0.vtstream;

public interface VTStreamMessage<T> {
    T payload();
    enum Flag {
        NORMAL,
        TERMINAL
    }
}
