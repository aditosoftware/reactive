package de.adito.util.reactive.cache;

import de.adito.util.reactive.AbstractListenerObservable;
import io.reactivex.rxjava3.core.Observable;
import io.reactivex.rxjava3.disposables.Disposable;
import io.reactivex.rxjava3.subjects.BehaviorSubject;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.*;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for the {@link ObservableCache} class
 *
 * @author m.kaspera, 28.02.2023
 * @author w.glanzer, 11.12.2018
 */
class ObservableCacheTest
{
  private final String testKey = "test";
  private ObservableCache cache;

  @BeforeEach
  void setUp()
  {
    cache = new ObservableCache();
  }

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

  @Test
  void test_calculate()
  {
    cache.calculateSequential("test_calculate", () -> Observable.just(1, 2, 3));
    cache.calculateSequential("test_calculate", () -> Observable.just(2, 3, 4, 5));
    Observable<Integer> obs2 = cache.calculateSequential("test_calculate", () -> Observable.just(6, 2, 3, 4));

    Integer integer = obs2.blockingFirst();
    Assertions.assertEquals(3, integer.intValue());
  }

  @Test
  void test_calculate_concurrent()
  {
    //noinspection RedundantCast,unchecked,rawtypes
    new Thread(() -> cache.calculateSequential("id1", () -> (Observable) Observable.just(1, 2, 3)
        .map(pInt -> {
          Thread.sleep(500);
          return pInt;
        }))).start();
    new Thread(() -> {
      try
      {
        Thread.sleep(100);
      }
      catch (InterruptedException ignored)
      {
        // nothing
      }
      cache.calculateSequential("id1", () -> {
        Assertions.fail("The second observable must not be called, cause the other one was registered before");
        return Observable.empty();
      });
    }).start();
  }

  @Test
  void test_dispose()
  {
    AtomicBoolean intervalDisposed = new AtomicBoolean(false);
    Observable<Long> interval = Observable.interval(100, TimeUnit.MILLISECONDS)
        .doOnDispose(() -> intervalDisposed.set(true));
    Observable<Long> cachedInterval = cache.calculateSequential("test", () -> interval);

    // Do not dispose underlying observable on single subscription dispose
    cachedInterval.subscribe().dispose();
    Assertions.assertFalse(intervalDisposed.get());

    // "long living" disposable
    Disposable disposable = cachedInterval.subscribe();

    // dispose underyling observable on ObservableCache dispose
    new ObservableCacheDisposable(cache).dispose();
    Assertions.assertTrue(intervalDisposed.get());
    Assertions.assertTrue(disposable.isDisposed());
  }

  @Test
  void test_changeSizeOnDispose()
  {
    Observable<Long> test = cache.calculateSequential("test", () -> Observable.interval(100, TimeUnit.MILLISECONDS));
    Assertions.assertEquals(1, cache.size());

    cache.calculateSequential("test2", () -> Observable.interval(100, TimeUnit.MILLISECONDS));
    cache.calculateSequential("test3", () -> Observable.interval(100, TimeUnit.MILLISECONDS));
    cache.calculateSequential("test4", () -> Observable.interval(100, TimeUnit.MILLISECONDS));
    Assertions.assertEquals(4, cache.size());

    test.subscribe();
    Assertions.assertEquals(4, cache.size());

    cache.calculateSequential("test", () -> Observable.interval(100, TimeUnit.MILLISECONDS));
    cache.calculateSequential("test2", () -> Observable.interval(100, TimeUnit.MILLISECONDS));
    Assertions.assertEquals(4, cache.size());

    new ObservableCacheDisposable(cache).dispose();
    Assertions.assertEquals(0, cache.size());
  }

  @Test
  void test_calculate_eviction_maxEntries()
  {
    ObservableCache cache = ObservableCache.createWithMaxUnsubscribedElements(3);

    // fill up cache
    cache.calculateSequential(1, () -> Observable.interval(100, TimeUnit.MILLISECONDS));
    Observable<Long> valueToListenOn = cache.calculateSequential("listened", () -> Observable.interval(100, TimeUnit.MILLISECONDS));
    cache.calculateSequential(3, () -> Observable.interval(100, TimeUnit.MILLISECONDS));

    // listen to a value
    Disposable disposable = valueToListenOn.subscribe(pInt -> {
    });

    // randomize cache
    for (int i = 0; i < 500; i++)
      cache.calculateSequential("random" + i, () -> Observable.interval(100, TimeUnit.MILLISECONDS));

    // evaluate, if evicted
    cache.calculateSequential("listened", () -> {
      Assertions.fail("Observable was evicted, even though there was a subscription to it");
      return Observable.empty();
    });

    // dispose
    disposable.dispose();

    // randomize cache
    for (int i = 0; i < 500; i++)
      cache.calculateSequential("random" + i, () -> Observable.interval(100, TimeUnit.MILLISECONDS));

    // evaluate now -> should be evicted, because of dispose above
    AtomicBoolean created = new AtomicBoolean(false);
    cache.calculateSequential("listened", () -> {
      created.set(true);
      return Observable.empty();
    });
    Assertions.assertTrue(created.get());
  }

  @Test
  void test_calculation_initiallyBroken()
  {
    Exception value = new Exception();

    // Create an observable that throws an error
    Observable<Object> test = cache.calculateSequential("test", () -> Observable.error(value));

    try
    {
      // Try to get the value - this will correctly fail
      //noinspection ResultOfMethodCallIgnored
      test.blockingFirst();
      Assertions.fail();
    }
    catch (Exception e)
    {
      // The cause of the catched exception should be our "real" value inside the observable
      Assertions.assertEquals(value, e.getCause());
    }
  }

  @ParameterizedTest(name = "errorOnNext delay: {arguments} ms")
  @ValueSource(ints = {0, 2, 5, 10, 100, 1000})
  void test_calculation_errorOnNext(int pDelayMs)
  {
    // Create our own exception class
    class MyException extends RuntimeException
    {
    }

    BehaviorSubject<Integer> subject = BehaviorSubject.createDefault(0);
    Supplier<Observable<Integer>> observableCreator = () -> subject
        .switchMap(pInt -> {
          if (pDelayMs > 0)
            Thread.sleep(pDelayMs); //Slow
          if (pInt == 3)
            throw new MyException();
          return Observable.just(pInt);
        });

    Observable<Integer> cached1 = cache.calculateSequential("test", observableCreator);
    subject.onNext(1);
    subject.onNext(2);
    subject.onNext(3);

    //noinspection ResultOfMethodCallIgnored
    Assertions.assertThrows(MyException.class, cached1::blockingLast);
    subject.onNext(0); // reset

    Observable<Integer> cached2 = cache.calculateSequential("test", observableCreator);
    Assertions.assertNotEquals(cached1, cached2);
  }

  @ParameterizedTest(name = "errorOnSubscribe delay: {arguments} ms")
  @ValueSource(ints = {0, 2, 5, 10, 100, 1000})
  void test_calculation_errorOnSubscribe(int pDelayMs)
  {
    // Create our own exception class
    class MyException extends RuntimeException
    {
    }

    Supplier<Observable<Integer>> observableCreator = () -> Observable.create(new AbstractListenerObservable<Object, Object, Integer>(new Object())
    {
      @NotNull
      @Override
      protected Object registerListener(@NotNull Object pListenableValue, @NotNull IFireable<Integer> pFireable)
      {
        try
        {
          if (pDelayMs > 0)
            Thread.sleep(pDelayMs);
        }
        catch (Exception e)
        {
          // ignore
        }

        throw new MyException();
      }

      @Override
      protected void removeListener(@NotNull Object pListenableValue, @NotNull Object pO)
      {
        // ignore
      }
    });

    Observable<Integer> cached1 = cache.calculateSequential("test", observableCreator);

    Assertions.assertThrows(MyException.class, cached1::blockingSubscribe);

    Observable<Integer> cached2 = cache.calculateSequential("test", observableCreator);
    Assertions.assertNotEquals(cached1, cached2);
  }

  @Test
  void test_blockingFirstOnDisposedCache()
  {
    BehaviorSubject<String> subject = BehaviorSubject.create();
    Observable<String> cachedObservable = cache.calculateSequential("test", () -> subject);
    subject.onNext("first");
    subject.onNext("second");

    // Initial Value - observable should be cached, because of undisposed cache
    Assertions.assertEquals("second", cachedObservable.blockingFirst());
    Assertions.assertNotEquals(subject, cachedObservable);

    // Dispose Cache
    new ObservableCacheDisposable(cache).dispose();

    // Now the returned observable of the cache should be completed and fail on blockingFirst, because it won't emit items anymore
    //noinspection ResultOfMethodCallIgnored
    Assertions.assertThrows(NoSuchElementException.class, cachedObservable::blockingFirst);
  }

  @Test
  void test_addObservableToCacheAfterDispose()
  {
    // add observable to cache - should return a new observable, a cached one
    BehaviorSubject<String> cached = BehaviorSubject.create();
    Assertions.assertNotEquals(cached, cache.calculateSequential("test", () -> cached));

    // Dispose Cache
    new ObservableCacheDisposable(cache).dispose();

    // If we ask the cache to cache an observable after cache dispose, it should return its input, because the cache was already disposed.
    BehaviorSubject<String> uncached = BehaviorSubject.create();
    Assertions.assertEquals(uncached, cache.calculateSequential("testUncached", () -> uncached));
  }

  @ParameterizedTest
  @ValueSource(strings = {"parallel", "sequential"})
  void test_addObservableMultithreaded(String pType) throws Exception
  {
    AtomicBoolean waitObj = new AtomicBoolean(true);
    AtomicInteger created = new AtomicInteger(0);
    AtomicInteger result = new AtomicInteger(0);
    int count = 100;

    for (int i = 0; i < count; i++)
    {
      new Thread(() -> {
        try
        {
          while (waitObj.get())
          {
            // wait until ready
            synchronized (waitObj)
            {
              waitObj.wait();
            }
          }

          Supplier<Observable<Optional<Integer>>> observableSupplier = () -> {
            created.incrementAndGet();
            return BehaviorSubject.createDefault(Optional.of(1));
          };
          Observable<Optional<Integer>> observable;

          if (pType.equals("parallel"))
            observable = cache.calculateParallel("multithreaded", observableSupplier);
          else
            observable = cache.calculateSequential("multithreaded", observableSupplier);

          observable
              .blockingFirst()
              .ifPresent(result::addAndGet);
        }
        catch (Exception e)
        {
          // ignore
        }
      }).start();
    }

    // Await Thread creation
    Thread.sleep(1000);

    // trigger all waiting threads
    synchronized (waitObj)
    {
      waitObj.set(false);
      waitObj.notifyAll();
    }

    // await finish
    Thread.sleep(2000);

    Assertions.assertEquals(count, result.get());
    Assertions.assertEquals(1, created.get());
  }
}