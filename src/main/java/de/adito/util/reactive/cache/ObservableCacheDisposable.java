package de.adito.util.reactive.cache;

import io.reactivex.rxjava3.disposables.Disposable;
import org.jetbrains.annotations.NotNull;

import java.lang.ref.WeakReference;

/**
 * Disposable to be able to dispose an ObservableCache.
 * It holds a weak reference to the underlying cache.
 *
 * @see ObservableCache
 * @author w.glanzer, 26.12.2019
 */
public class ObservableCacheDisposable implements Disposable
{

  private WeakReference<ObservableCache> cacheRef;

  public ObservableCacheDisposable(@NotNull ObservableCache pCache)
  {
    cacheRef = new WeakReference<>(pCache);
  }

  @Override
  public void dispose()
  {
    if (isDisposed())
      return;

    ObservableCache cache = cacheRef.get();
    if (cache != null)
    {
      cacheRef = null;
      cache.invalidate();
    }
  }

  @Override
  public boolean isDisposed()
  {
    return cacheRef == null || cacheRef.get() == null;
  }

}
