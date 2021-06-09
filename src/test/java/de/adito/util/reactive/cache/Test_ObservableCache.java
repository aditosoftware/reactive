package de.adito.util.reactive.cache;

import io.reactivex.rxjava3.core.Observable;
import io.reactivex.rxjava3.disposables.Disposable;
import org.junit.jupiter.api.*;

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
    Assertions.assertEquals(1, integer.intValue());
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
  void test_calculate_eviction_maxEntries()
  {
    ObservableCache cache = ObservableCache.createWithMaxUnsubscribedElements(3);

    // fill up cache
    cache.calculate(1, () -> Observable.timer(100, TimeUnit.MILLISECONDS));
    Observable<Long> valueToListenOn = cache.calculate("listened", () -> Observable.timer(100, TimeUnit.MILLISECONDS));
    cache.calculate(3, () -> Observable.timer(100, TimeUnit.MILLISECONDS));

    // listen to a value
    Disposable disposable = valueToListenOn.subscribe(pInt -> {
    });

    // randomize cache
    for(int i = 0; i < 500; i++)
      cache.calculate("random" + i, () -> Observable.timer(100, TimeUnit.MILLISECONDS));

    // evaluate, if evicted
    cache.calculate("listened", () -> {
      Assertions.fail("Observable was evicted, even though there was a subscription to it");
      return Observable.empty();
    });

    // dispose
    disposable.dispose();

    // randomize cache
    for(int i = 0; i < 500; i++)
      cache.calculate("random" + i, () -> Observable.timer(100, TimeUnit.MILLISECONDS));

    // evaluate now -> should be evicted, because of dispose above
    AtomicBoolean created = new AtomicBoolean(false);
    cache.calculate("listened", () -> {
      created.set(true);
      return Observable.empty();
    });
    Assertions.assertTrue(created.get());
  }
}
