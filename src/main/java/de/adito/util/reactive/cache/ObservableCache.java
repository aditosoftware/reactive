package de.adito.util.reactive.cache;

import io.reactivex.Observable;
import io.reactivex.disposables.CompositeDisposable;
import org.jetbrains.annotations.NotNull;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

/**
 * Cache to store RxJava Observables
 *
 * @author w.glanzer, 11.12.2018
 */
public class ObservableCache
{

  private final Map<Object, Observable<?>> cache = new ConcurrentHashMap<>();
  private final CompositeDisposable compositeDisposable = new CompositeDisposable();

  /**
   * Creates a new Entry in our Observable Cache.
   * If an Entry with pIdentifier was already created, the previous instance will be returned.
   * Otherwise the Supplier gets called and a new Observable will be created and shared with a ReplaySubject(!)
   *
   * @param pIdentifier Identifier
   * @param pObservable Observable to cache
   * @return the cached observable
   */
  @NotNull
  public synchronized <T> Observable<T> calculate(@NotNull Object pIdentifier, @NotNull Supplier<Observable<T>> pObservable)
  {
    //noinspection unchecked We do not have a method to check generic-validity
    return (Observable<T>) cache.computeIfAbsent(pIdentifier, pID -> pObservable.get()
        .replay(1)
        .autoConnect(0, compositeDisposable::add));
  }

  /**
   * Disposes all Subjects and clears the underlying cache
   */
  public synchronized void invalidate()
  {
    Exception ex = null;
    try
    {
      compositeDisposable.clear();
    }
    catch (Exception pE)
    {
      ex = pE;
    }
    finally
    {
      Set<Map.Entry<Object, Observable<?>>> entries = new HashSet<>(cache.entrySet());
      for (Map.Entry<Object, Observable<?>> entry : entries)
      {
        cache.remove(entry.getKey());
      }
    }

    if (ex != null)
      throw new RuntimeException("Failed to clear cache completely. All Entries have been removed, " +
                                     "but meanwhile an exception was thrown. See cause for more information", ex);
  }

}
