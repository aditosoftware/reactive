package de.adito.util.reactive.cache;

import io.reactivex.Observable;
import io.reactivex.subjects.*;
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

  private final Map<Object, Subject<?>> cache = new ConcurrentHashMap<>();

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
  public <T> Observable<T> calculate(@NotNull Object pIdentifier, @NotNull Supplier<Observable<T>> pObservable)
  {
    //noinspection unchecked We do not have a method to check generic-validity
    return (Observable<T>) cache.computeIfAbsent(pIdentifier, pID -> pObservable.get()
        .share()
        .subscribeWith(ReplaySubject.createWithSize(1)));
  }

  /**
   * Completes all Subjects and clears the underlying cache
   */
  public void invalidate()
  {
    Exception ex = null;
    Set<Map.Entry<Object, Subject<?>>> entries = new HashSet<>(cache.entrySet());
    for (Map.Entry<Object, Subject<?>> entry : entries)
    {
      try
      {
        entry.getValue().onComplete();
      }
      catch(Exception e)
      {
        ex = e;
      }
      finally
      {
        cache.remove(entry.getKey());
      }
    }

    if(ex != null)
      throw new RuntimeException("Failed to clear cache completely. All Entries have been removed, " +
                                     "but meanwhile an exception was thrown. See cause for more information", ex);
  }

}
