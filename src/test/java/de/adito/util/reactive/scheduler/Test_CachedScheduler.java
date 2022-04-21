package de.adito.util.reactive.scheduler;

import io.reactivex.rxjava3.core.Observable;
import io.reactivex.rxjava3.core.*;
import io.reactivex.rxjava3.disposables.Disposable;
import org.junit.jupiter.api.Test;

import java.util.*;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * @author w.glanzer, 13.04.2022
 */
public class Test_CachedScheduler
{

  private final Scheduler scheduler = CachedScheduler.getInstance();

  @Test
  void shouldNotBlockEndless() throws InterruptedException
  {
    List<Object> receivedObjects = Collections.synchronizedList(new ArrayList<>());

    // Spam the scheduler to do stuff
    for (int i = 0; i < 8; i++)
      scheduler.scheduleDirect(() -> {
        try
        {
          Thread.sleep(500);
        }
        catch (Exception e)
        {
          // ignore
        }
      });

    // Schedule real work
    for (int i = 0; i < Runtime.getRuntime().availableProcessors(); i++)
      scheduler.scheduleDirect(() -> receivedObjects.add(Observable.create(emitter -> emitter.setDisposable(Disposable.empty()))
                                                             .startWithItem(123)
                                                             .observeOn(scheduler)
                                                             .subscribeOn(scheduler)
                                                             .throttleLatest(500, TimeUnit.MILLISECONDS)
                                                             .blockingFirst()));

    // await all threads ok
    Thread.sleep(5000);

    // check if all runnable fired
    assertEquals(10, receivedObjects.size());
    assertTrue(receivedObjects.stream().allMatch(pObj -> Objects.equals(123, pObj)), receivedObjects.toString());
  }

}
