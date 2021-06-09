package de.adito.util.reactive.cache;

import com.google.common.cache.*;
import com.google.common.collect.*;
import io.reactivex.rxjava3.core.Observable;
import io.reactivex.rxjava3.core.*;
import io.reactivex.rxjava3.disposables.Disposable;
import io.reactivex.rxjava3.functions.Function;
import org.jetbrains.annotations.*;

import java.util.*;
import java.util.concurrent.*;
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
  private final Multimap<Object, Disposable> disposableRegistry = ArrayListMultimap.create();
  private final Cache<Object, CacheValue<?>> cache;

  /**
   * Creates an ObservableCache with a maximum number of "unsubscribed" elements.
   * All subscribed elements will not be counted.
   *
   * @param pMaxUnsubscribedElements maximum number of unsubscribed elements
   * @return the cache instance
   */
  @NotNull
  public static ObservableCache createWithMaxUnsubscribedElements(long pMaxUnsubscribedElements)
  {
    if(pMaxUnsubscribedElements <= 0)
      throw new IllegalArgumentException("The amount of unsubscribed elements must be larger than 0, because every observable will be " +
                                             "unsubscribed at the beginning.");

    return new ObservableCache(CacheBuilder.newBuilder()
                                   .weigher(new CacheValue.SubCountWeigher_IgnoreSubscribedElements())
                                   .maximumWeight(pMaxUnsubscribedElements));
  }

  public ObservableCache()
  {
    //noinspection unchecked,rawtypes
    this((CacheBuilder) CacheBuilder.newBuilder());
  }

  private ObservableCache(@NotNull CacheBuilder<Object, CacheValue<?>> pCache)
  {
    cache = pCache
        .removalListener(new _RemovalListener())
        .build();
  }

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
    try
    {
      //noinspection unchecked We do not have a method to check generic-validity
      return (Observable<T>) cache.get(pIdentifier, () -> new CacheValue<>(_create(pIdentifier, pObservable, null)
                                                                               // serialize it, because AbstractListenerObservables (for example)
                                                                               // can fire new values async
                                                                               .serialize()
                                                                               .replay(1)
                                                                               .autoConnect(1, pDis -> disposableRegistry.put(pIdentifier, pDis)))
          // re-evaluate the caches weigh of a value, if the subscription count changes
          .doOnSubscriptionCountChange(pValue -> cache.put(pIdentifier, pValue)))
          .getObservable();
    }
    catch (ExecutionException e)
    {
      throw new RuntimeException("Failed to calculate cache value", e);
    }
  }

  /**
   * Factory-Method to create a new observable instance.
   * If this method was called too often, it will throw an error.
   *
   * @param pObservableSupplier Supplier to get new observables
   * @return Observable
   */
  @NotNull
  private synchronized <T> Observable<T> _create(@NotNull Object pIdentifier, @NotNull Supplier<Observable<T>> pObservableSupplier,
                                                 @Nullable Throwable pException)
  {
    long current = System.currentTimeMillis();
    Long previous = creationTimestamps.put(pIdentifier, current);

    // Prevent this method from beeing called too often
    if (pException != null && previous != null && (current - previous) < _CREATION_COOLDOwN_MS)
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
    try
    {
      cache.invalidateAll();
    }
    catch (Exception e)
    {
      throw new RuntimeException("Failed to clear observable cache completely. See cause for more information", e);
    }
  }

  /**
   * Listener that disposes all disposables, if the cached entry will be invalidated
   */
  private class _RemovalListener implements RemovalListener<Object, CacheValue<?>>
  {
    @Override
    public void onRemoval(@NotNull RemovalNotification<Object, CacheValue<?>> pNotification)
    {
      if(pNotification.getCause() == RemovalCause.REPLACED)
        return;

      Collection<Disposable> removedDisposables = disposableRegistry.removeAll(pNotification.getKey());
      if(removedDisposables != null)
        removedDisposables.forEach(pDisposable -> {
          if(!pDisposable.isDisposed())
            pDisposable.dispose();
        });
    }
  }
}
