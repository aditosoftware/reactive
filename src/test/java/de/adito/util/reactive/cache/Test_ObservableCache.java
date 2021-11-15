package de.adito.util.reactive.cache;

import io.reactivex.rxjava3.core.Observable;
import io.reactivex.rxjava3.disposables.Disposable;
import io.reactivex.rxjava3.subjects.BehaviorSubject;
import org.junit.jupiter.api.*;

import java.util.NoSuchElementException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author w.glanzer, 11.12.2018
 */
public class Test_ObservableCache
{

  private ObservableCache cache;

  @BeforeEach
  void setUp()
  {
    cache = new ObservableCache();
  }

  @Test
  void test_calculate()
  {
    cache.calculate("test_calculate", () -> Observable.just(1, 2, 3));
    cache.calculate("test_calculate", () -> Observable.just(2, 3, 4, 5));
    Observable<Integer> obs2 = cache.calculate("test_calculate", () -> Observable.just(6, 2, 3, 4));

    Integer integer = obs2.blockingFirst();
    Assertions.assertEquals(3, integer.intValue());
  }

  @Test
  void test_calculate_concurrent()
  {
    //noinspection RedundantCast,unchecked,rawtypes
    new Thread(() -> cache.calculate("id1", () -> (Observable) Observable.just(1, 2, 3)
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
      cache.calculate("id1", () -> {
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
    cache.calculate(1, () -> Observable.interval(100, TimeUnit.MILLISECONDS));
    Observable<Long> valueToListenOn = cache.calculate("listened", () -> Observable.interval(100, TimeUnit.MILLISECONDS));
    cache.calculate(3, () -> Observable.interval(100, TimeUnit.MILLISECONDS));

    // listen to a value
    Disposable disposable = valueToListenOn.subscribe(pInt -> {
    });

    // randomize cache
    for (int i = 0; i < 500; i++)
      cache.calculate("random" + i, () -> Observable.interval(100, TimeUnit.MILLISECONDS));

    // evaluate, if evicted
    cache.calculate("listened", () -> {
      Assertions.fail("Observable was evicted, even though there was a subscription to it");
      return Observable.empty();
    });

    // dispose
    disposable.dispose();

    // randomize cache
    for (int i = 0; i < 500; i++)
      cache.calculate("random" + i, () -> Observable.interval(100, TimeUnit.MILLISECONDS));

    // evaluate now -> should be evicted, because of dispose above
    AtomicBoolean created = new AtomicBoolean(false);
    cache.calculate("listened", () -> {
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
    Observable<Object> test = cache.calculate("test", () -> Observable.error(value));

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
}
