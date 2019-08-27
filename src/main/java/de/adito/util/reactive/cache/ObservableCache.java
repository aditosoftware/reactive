package de.adito.util.reactive.cache;

import io.reactivex.*;
import io.reactivex.Observable;
import io.reactivex.disposables.CompositeDisposable;
import io.reactivex.functions.Function;
import io.reactivex.subjects.BehaviorSubject;
import org.jetbrains.annotations.*;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

/**
 * Cache to store RxJava Observables
 *
 * @author w.glanzer, 11.12.2018
 */
public class ObservableCache
{
  private static final int _CREATION_COOLDOwN_MS = 200;
  private final Map<Object, Long> creationTimestamps = new ConcurrentHashMap<>();
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
    return (Observable<T>) cache.computeIfAbsent(pIdentifier, pID -> _create(pID, pObservable, null)
        .replay(1)
        .autoConnect(0, compositeDisposable::add));
  }

  /**
   * Factory-Method to create a new observable instance.
   * If this method was called too often, it will throw an error.
   *
   * @param pObservableSupplier Supplier to get new observables
   * @return Observable
   */
  @NotNull
  private synchronized  <T> Observable<T> _create(@NotNull Object pIdentifier, @NotNull Supplier<Observable<T>> pObservableSupplier,
                                                  @Nullable Throwable pException)
  {
    long current = System.currentTimeMillis();
    Long previous = creationTimestamps.put(pIdentifier, current);

    // Prevent this method from beeing called too often
    if(pException != null && previous != null && (current - previous) < _CREATION_COOLDOwN_MS)
      return Observable.error(new ObservableCacheRecursiveCreationException("An observable was prevented from beeing created too often, during " +
                                                                                _CREATION_COOLDOwN_MS + "ms. An exception was thrown during creation",
                                                                            pException));

    return pObservableSupplier.get()
        .onErrorResumeNext((Function<Throwable, ObservableSource<T>>) pEx -> _create(pIdentifier, pObservableSupplier, pEx));
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
