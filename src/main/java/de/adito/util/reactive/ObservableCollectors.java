package de.adito.util.reactive;

import io.reactivex.rxjava3.core.Observable;
import org.jetbrains.annotations.NotNull;

import java.util.*;
import java.util.function.*;
import java.util.stream.Collector;

/**
 * @author w.glanzer, 04.12.2018
 */
public class ObservableCollectors
{

  private ObservableCollectors()
  {
  }

  /**
   * Returns a collector which combines an Stream of Observables to one observable.
   * It is combined via Observable.combineLatest()
   *
   * @return the Collector
   */
  @NotNull
  public static <T> Collector<Observable<T>, ?, Observable<List<T>>> combineToList()
  {
    return _combine(pData -> {
      List<T> values = new ArrayList<>();
      for (Object data : pData)
        values.add((T) data);
      return values;
    });
  }

  /**
   * Returns a collector which combines an Stream of Observables to one observable.
   * It is combined via Observable.combineLatest()
   *
   * @return the Collector
   */
  @NotNull
  public static <T> Collector<Observable<Optional<T>>, ?, Observable<List<T>>> combineOptionalsToList()
  {
    return _combine(pData -> {
      List<T> values = new ArrayList<>();
      for (Object data : pData)
        ((Optional<T>) data).ifPresent(values::add);
      return values;
    });
  }

  /**
   * Returns a collector which combines an Stream of Observables to one observable.
   * It is combined via Observable.combineLatest()
   *
   * @return the Collector
   */
  @NotNull
  public static <T> Collector<Observable<List<T>>, ?, Observable<List<T>>> combineListToList()
  {
    return _combine(pData -> {
      List<T> values = new ArrayList<>();
      for (Object data : pData)
        values.addAll(((List<T>) data));
      return values;
    });
  }

  /**
   * Returns an Collector which combines Observables to one Observable with a value-List
   *
   * @param pFn   Function, which combines the Input-Values to a List of output-Values
   * @param <IN>  INPUT-Value-Type
   * @param <OUT> OUTPUT-Value-Type
   * @return the Collector, not <tt>null</tt>
   */
  @NotNull
  private static <IN, OUT> Collector<Observable<IN>, ?, Observable<List<OUT>>> _combine(@NotNull Function<Object[], List<OUT>> pFn)
  {
    return Collector.of((Supplier<ArrayList<Observable<IN>>>) ArrayList::new, ArrayList::add, (pList1, pList2) -> {
      ArrayList<Observable<IN>> copy = new ArrayList<>(pList1);
      copy.addAll(pList2);
      return copy;
    }, pList -> {
      if (pList.isEmpty())
        return Observable.just(Collections.emptyList());
      return Observable.combineLatest(pList, pFn::apply);
    });
  }

}
