package de.adito.util.reactive.cache;

import io.reactivex.Observable;
import org.junit.jupiter.api.*;

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
    new Thread(() -> cache.calculate("id1", () -> Observable.just(1, 2, 3)
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

}
