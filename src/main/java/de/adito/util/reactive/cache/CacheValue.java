package de.adito.util.reactive.cache;

import com.google.common.cache.Weigher;
import io.reactivex.rxjava3.core.Observable;
import org.jetbrains.annotations.NotNull;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

/**
 * Value-Wrapper that encapsulates an observable and its subscription count
 *
 * @author w.glanzer, 09.06.2021
 */
class CacheValue<T>
{

  private final AtomicInteger subCounter = new AtomicInteger(0);
  private final Observable<T> observable;
  private Consumer<CacheValue<T>> onChange;

  public CacheValue(@NotNull Observable<T> pObservable)
  {
    observable = pObservable
        .doOnSubscribe(pDis -> {
          subCounter.incrementAndGet();
          if(onChange != null)
            onChange.accept(this);
        })
        .doOnTerminate(() -> {
          subCounter.decrementAndGet();
          if(onChange != null)
            onChange.accept(this);
        })
        .doOnDispose(() -> {
          subCounter.decrementAndGet();
          if(onChange != null)
            onChange.accept(this);
        });
  }

  /**
   * The given function will be executed, if the subscription count changes
   *
   * @param pOnChange function to execute on change
   * @return this
   */
  @NotNull
  public CacheValue<T> doOnSubscriptionCountChange(@NotNull Consumer<CacheValue<T>> pOnChange)
  {
    onChange = pOnChange;
    return this;
  }

  /**
   * @return the inner observable
   */
  @NotNull
  public Observable<T> getObservable()
  {
    return observable;
  }

  /**
   * Uses the subscription count of a cacheValue to calculate its weight.
   * All elements with a subcount larger than 0 will result in weight = 0
   */
  public static class SubCountWeigher_IgnoreSubscribedElements implements Weigher<Object, CacheValue<?>>
  {
    @Override
    public int weigh(@NotNull Object pKey, @NotNull CacheValue<?> pValue)
    {
      return calculateWeight(pValue.subCounter.get());
    }

    public static int calculateWeight(int pSubCounter)
    {
      if (pSubCounter == 0)
        return 1;
      return 0;
    }
  }
}
