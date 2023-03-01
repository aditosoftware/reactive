package de.adito.util.reactive.cache;

import io.reactivex.rxjava3.core.Observable;
import io.reactivex.rxjava3.disposables.Disposable;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Test;

import java.util.*;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for the {@link ObservableCache} class
 *
 * @author m.kaspera, 28.02.2023
 */
class ObservableCacheTest
{
  private final String testKey = "test";


  /**
   * Tests if the cache allows the creation of 40 new Observable values if an observable repeatedly fails.
   * Before, this was capped at 30 values per 200ms
   */
  @Test
  void isCanCreate50RecursiveObservableCalls()
  {
    ObservableCache observableCache = new ObservableCache();
    String someRandomValue = "a";

    List<String> groundTruth = new ArrayList<>();
    for(int counter = 0; counter < 40; counter++) {
      groundTruth.add(someRandomValue);
    }
    groundTruth.add(testKey);

    List<String> returnedElements = new ArrayList<>();
    //noinspection ConstantConditions we want to produce an exception here, so we need to do some wonky stuff
    Observable<String> testObservable = getErrorHandled(0, () -> observableCache
        .calculateSequential(testKey, () -> Observable.just(0)
            .map(pInteger -> Optional.ofNullable(null).get().toString())
            .startWithItem(someRandomValue)));

    Disposable disposable = testObservable.subscribe(returnedElements::add);

    assertEquals(groundTruth, returnedElements);

    disposable.dispose();
  }

  /**
   * Helper for creating a capped recursive observable
   *
   * @param pCounter Counter, used to stop the recursive behaviour at some point, we do not want to provoke a stackoverflow
   * @param pObservableSupplier Supplies the observable used as initial value and onError case
   * @return Recursive Observable that creates itself anew each time it runs on an error (which it does after the initial value). Recursion is capped
   * at 40 total produced values
   */
  private Observable<String> getErrorHandled(int pCounter, @NotNull Supplier<Observable<String>> pObservableSupplier)
  {
    // no pCounter++ because that would change pCounter, which means it no longer is considered final for the lambda below
    int counterIncrement = pCounter + 1;
    // smaller 39 instead of 40 here because the initial value already adds a value, the onError is only the second value here
    return pObservableSupplier.get()
        .onErrorResumeNext(pEx -> pCounter < 39 ? getErrorHandled(counterIncrement, pObservableSupplier) : Observable.just(testKey));
  }

  /**
   * Tests that the cache returns the same item for the same key if the observable is valid
   */
  @Test
  void isCacheWorking()
  {
    ObservableCache observableCache = new ObservableCache();

    Observable<Integer> observable = observableCache.calculateSequential(testKey, () -> Observable.just(0));
    Observable<Integer> observable2 = observableCache.calculateSequential(testKey, () -> Observable.just(1));

    assertAll(() -> assertEquals(observable, observable2),
              () -> assertEquals(0, (int) observable2.blockingFirst()));
  }

  /**
   * Tests if an observable that fails due to an exception and becomes invalid is evicted from the cache, leading to a new observable being created
   * when the cache is called with the same key
   */
  @SuppressWarnings("ConstantConditions") // needed to provoke an exception for the test
  @Test
  void isObjectDiscardedOnError()
  {
    ObservableCache observableCache = new ObservableCache();

    Observable<String> observable = observableCache.calculateSequential(testKey, () -> Observable.just(0)
        .map(pInteger -> Optional.ofNullable(null).get().toString()));
    // subscribe so that the Observable chain ist created, leading to an error in the Observable, which makes the Observable invalid and should lead
    // to its eviction out of the cache
    observable.subscribe();
    // call the cache with the same key. This should lead to the cache returning a different Observable compared to the first one
    Observable<String> observable2 = observableCache.calculateSequential(testKey, () -> Observable.just(testKey));

    assertAll(() -> assertNotEquals(observable2, observable),
              () -> assertEquals(testKey, observable2.blockingFirst()));
  }
}