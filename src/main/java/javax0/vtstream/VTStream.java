package javax0.vtstream;

public interface VTStream<T> {
  void perform(VTStreamMessage<T> message);
}
