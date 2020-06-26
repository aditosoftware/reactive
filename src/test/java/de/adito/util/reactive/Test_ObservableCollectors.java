package de.adito.util.reactive;

import io.reactivex.rxjava3.core.Observable;
import org.junit.jupiter.api.*;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

/**
 * @author w.glanzer, 07.12.2018
 */
public class Test_ObservableCollectors
{

  @Test
  void test_combineToList()
  {
    Observable<List<Integer>> observableList = Stream.of(Observable.just(1), Observable.just(2), Observable.just(3), Observable.just(4))
        .collect(ObservableCollectors.combineToList());

    AtomicInteger callCounter = new AtomicInteger(0);
    observableList.subscribe(pVal -> {
      Assertions.assertEquals(0, callCounter.getAndIncrement());
      Assertions.assertEquals(Arrays.asList(1, 2, 3, 4), pVal);
    }).dispose();
  }

  @Test
  void test_combineOptionalToList()
  {
    Observable<List<Integer>> observableList = Stream.of(Observable.just(Optional.of(1)), Observable.just(Optional.of(2)),
                                                         Observable.just(Optional.of(3)), Observable.just(Optional.of(4)))
        .collect(ObservableCollectors.combineOptionalsToList());

    AtomicInteger callCounter = new AtomicInteger(0);
    observableList.subscribe(pVal -> {
      Assertions.assertEquals(0, callCounter.getAndIncrement());
      Assertions.assertEquals(Arrays.asList(1, 2, 3, 4), pVal);
    }).dispose();
  }

  @Test
  void test_combineListToList()
  {
    Observable<List<Integer>> observableList = Stream.of(Observable.just(Arrays.asList(1, 2)), Observable.just(Collections.singletonList(2)),
                                                         Observable.just(Collections.singletonList(3)), Observable.just(Collections.singletonList(4)))
        .collect(ObservableCollectors.combineListToList());

    AtomicInteger callCounter = new AtomicInteger(0);
    observableList.subscribe(pVal -> {
      Assertions.assertEquals(0, callCounter.getAndIncrement());
      Assertions.assertEquals(Arrays.asList(1, 2, 2, 3, 4), pVal);
    }).dispose();
  }

}
