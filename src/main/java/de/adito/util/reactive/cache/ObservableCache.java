package de.adito.util.reactive.cache;

import com.google.common.cache.*;
import com.google.common.collect.*;
import io.reactivex.rxjava3.core.Observable;
import io.reactivex.rxjava3.core.*;
import io.reactivex.rxjava3.disposables.Disposable;
import io.reactivex.rxjava3.functions.Function;
import io.reactivex.rxjava3.subjects.ReplaySubject;
import org.jetbrains.annotations.*;

import javax.annotation.concurrent.ThreadSafe;
import java.lang.reflect.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;
import java.util.logging.*;

/**
 * Cache to store RxJava Observables
 *
 * @author w.glanzer, 11.12.2018
 */
@ThreadSafe
public class ObservableCache
{
  private static final int _REQUESTS_TIMESLOT = 200;
  private static final int _REQUESTS_MAX_PER_TIMESLOT = 30;
  private static final Logger _LOGGER = Logger.getLogger(ObservableCache.class.getName());
  private static Field _BUFFER_FIELD;

  /* First of all we need a cache (the "underlying" cache) that contains all requests to a specific pIdentifier.
   * This cache should expire all entries at a given amount of time after initial write.
   * This cache gets wrapped with another cache, so that we are able to match the correct pIdentifier with the correct set of request times
   * It is roughly comparable with a self-expiring multimap inside a cache */
  private final LoadingCache<Object, LoadingCache<Object, Object>> requestCache = CacheBuilder.newBuilder()
      .expireAfterAccess(_REQUESTS_TIMESLOT * 2L, TimeUnit.MILLISECONDS)
      .removalListener((RemovalListener<Object, Cache<Object, Object>>) notification -> notification.getValue().invalidateAll())
      .build(new CacheLoader<Object, LoadingCache<Object, Object>>()
      {
        @Override
        public LoadingCache<Object, Object> load(@NotNull Object pCacheKey)
        {
          return CacheBuilder.newBuilder()
              .expireAfterWrite(_REQUESTS_TIMESLOT, TimeUnit.MILLISECONDS)
              .build(new CacheLoader<Object, Object>()
              {
                @Override
                public Object load(@NotNull Object key)
                {
                  return key;
                }
              });
        }
      });
  private final Multimap<Object, Disposable> disposableRegistry = Multimaps.synchronizedMultimap(ArrayListMultimap.create());
  private final Cache<Object, CacheValue<?>> cache;
  private final Scheduler observeScheduler;
  private final Scheduler subscribeScheduler;
  private final AtomicBoolean valid = new AtomicBoolean(true);

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
    return createWithMaxUnsubscribedElements(pMaxUnsubscribedElements, null, null);
  }

  /**
   * Creates an ObservableCache with a maximum number of "unsubscribed" elements.
   * All subscribed elements will not be counted.
   *
   * @param pMaxUnsubscribedElements maximum number of unsubscribed elements
   * @param pObserveScheduler        scheduler to be used during observe phase, NULL if you dont want to use a scheduler
   * @param pSubscribeScheduler      scheduler to be used during subscribe phase, NULL if you dont want to use a scheduler
   * @return the cache instance
   */
  @NotNull
  public static ObservableCache createWithMaxUnsubscribedElements(long pMaxUnsubscribedElements,
                                                                  @Nullable Scheduler pObserveScheduler, @Nullable Scheduler pSubscribeScheduler)
  {
    if (pMaxUnsubscribedElements <= 0)
      throw new IllegalArgumentException("The amount of unsubscribed elements must be larger than 0, because every observable will be " +
                                             "unsubscribed at the beginning.");

    return new ObservableCache(CacheBuilder.newBuilder()
                                   .weigher(new CacheValue.SubCountWeigher_IgnoreSubscribedElements())
                                   .maximumWeight(pMaxUnsubscribedElements), pObserveScheduler, pSubscribeScheduler);
  }

  public ObservableCache()
  {
    this(null, null);
  }

  public ObservableCache(@Nullable Scheduler pObserveScheduler, @Nullable Scheduler pSubscribeScheduler)
  {
    //noinspection unchecked,rawtypes
    this((CacheBuilder) CacheBuilder.newBuilder(), pObserveScheduler, pSubscribeScheduler);
  }

  private ObservableCache(@NotNull CacheBuilder<Object, CacheValue<?>> pCache,
                          @Nullable Scheduler pObserveScheduler, @Nullable Scheduler pSubscribeScheduler)
  {
    cache = pCache
        .removalListener(new _RemovalListener())
        .build();
    observeScheduler = pObserveScheduler;
    subscribeScheduler = pSubscribeScheduler;
  }

  /**
   * Creates a new Entry in our Observable Cache.
   * If an Entry with pIdentifier was already created, the previous instance will be returned.
   * Otherwise the Supplier gets called and a new Observable will be created and shared with a ReplaySubject(!).
   * The created subject gets parallelized with the observe/subscribe schedulers given in constructor, if available.
   * So that the cached observable executes its downstream subscriptions will be evaluated in parallel.
   *
   * @param pIdentifier Identifier
   * @param pObservable Observable to cache
   * @return the cached observable
   */
  @NotNull
  public <T> Observable<T> calculateParallel(@NotNull Object pIdentifier, @NotNull Supplier<Observable<T>> pObservable)
  {
    return calculateParallel(pIdentifier, pObservable, observeScheduler, subscribeScheduler);
  }

  /**
   * Creates a new Entry in our Observable Cache.
   * If an Entry with pIdentifier was already created, the previous instance will be returned.
   * Otherwise the Supplier gets called and a new Observable will be created and shared with a ReplaySubject(!)
   * The created subject gets parallelized with the observe/subscribe schedulers given in parameters.
   * So that the cached observable executes its downstream subscriptions will be evaluated in parallel.
   *
   * @param pIdentifier         Identifier
   * @param pObservable         Observable to cache
   * @param pObserveScheduler   Scheduler that gets used during observe
   * @param pSubscribeScheduler Scheduler that gets used during subscribe
   * @return the cached observable
   */
  @NotNull
  public <T> Observable<T> calculateParallel(@NotNull Object pIdentifier, @NotNull Supplier<Observable<T>> pObservable,
                                             @Nullable Scheduler pObserveScheduler, @Nullable Scheduler pSubscribeScheduler)
  {
    return _calculate(pIdentifier, pObservable, pObserveScheduler, pSubscribeScheduler);
  }

  /**
   * Creates a new Entry in our Observable Cache.
   * If an Entry with pIdentifier was already created, the previous instance will be returned.
   * Otherwise the Supplier gets called and a new Observable will be created and shared with a ReplaySubject(!).
   * The cached observable will execute its downstream subscriptions in sequence.
   *
   * @param pIdentifier Identifier
   * @param pObservable Observable to cache
   * @return the cached observable
   */
  @NotNull
  public <T> Observable<T> calculateSequential(@NotNull Object pIdentifier, @NotNull Supplier<Observable<T>> pObservable)
  {
    return _calculate(pIdentifier, pObservable, null, null);
  }

  /**
   * Creates a new Entry in our Observable Cache.
   * If an Entry with pIdentifier was already created, the previous instance will be returned.
   * Otherwise the Supplier gets called and a new Observable will be created and shared with a ReplaySubject(!)
   *
   * @param pIdentifier Identifier
   * @param pObservable Observable to cache
   * @return the cached observable
   * @deprecated Please decide if you want to use sequential {@link ObservableCache#calculateSequential(Object, Supplier)}
   * or parallel {@link ObservableCache#calculateParallel(Object, Supplier)} execution
   */
  @NotNull
  @Deprecated
  public <T> Observable<T> calculate(@NotNull Object pIdentifier, @NotNull Supplier<Observable<T>> pObservable)
  {
    return calculateSequential(pIdentifier, pObservable);
  }

  /**
   * @return the amount of currently cached observables
   */
  public long size()
  {
    return cache.size();
  }

  /**
   * Creates a new Entry in our Observable Cache.
   * If an Entry with pIdentifier was already created, the previous instance will be returned.
   * Otherwise the Supplier gets called and a new Observable will be created and shared with a ReplaySubject(!)
   *
   * @param pIdentifier         Identifier
   * @param pObservable         Observable to cache
   * @param pObserveScheduler   Scheduler that gets used during observe
   * @param pSubscribeScheduler Scheduler that gets used during subscribe
   * @return the cached observable
   */
  @NotNull
  private <T> Observable<T> _calculate(@NotNull Object pIdentifier, @NotNull Supplier<Observable<T>> pObservable,
                                       @Nullable Scheduler pObserveScheduler, @Nullable Scheduler pSubscribeScheduler)
  {
    try
    {
      if (!valid.get())
      {
        _LOGGER.log(Level.WARNING, "Calculating an observable inside an invalidated cache is not supported and " +
            "may lead to memory leaks, because a cache never gets disposed twice. The returned observable is not cached.",
                    new IllegalStateException());
        return pObservable.get();
      }

      //noinspection unchecked We do not have a method to check generic-validity
      return (Observable<T>) cache.get(pIdentifier, () -> {
        ReplaySubject<T> subject = ReplaySubject.createWithSize(1);
        Observable<T> observable = _createErrorHandled(pIdentifier, pObservable)
            .serialize()// serialize it, because AbstractListenerObservables (for example) can fire new values async
            .replay(1)
            .autoConnect(1, pDis -> disposableRegistry.put(pIdentifier, pDis))
            .subscribeWith(subject);
        disposableRegistry.put(pIdentifier, new Disposable()
        {
          @Override
          public void dispose()
          {
            subject.onComplete();
            _trimSizeBoundBuffer(subject);
          }

          @Override
          public boolean isDisposed()
          {
            return false;
          }
        });

        if (pObserveScheduler != null)
          observable = observable.observeOn(pObserveScheduler);

        if (pSubscribeScheduler != null)
          observable = observable.subscribeOn(pSubscribeScheduler);

        return new CacheValue<>(observable)
            // re-evaluate the caches weigh of a value, if the subscription count changes.
            .doOnSubscriptionCountChange(pValue -> {
              if(cache.getIfPresent(pIdentifier) != null)
                cache.put(pIdentifier, pValue);
            });
      }).getObservable();
    }
    catch (ExecutionException e)
    {
      throw new RuntimeException("Failed to calculate cache value", e);
    }
  }

  /**
   * Factory-Method to create a new observable instance.
   * Cares about handling errors and recreation of the observable chain.
   *
   * @param pObservableSupplier Supplier to get new observables
   * @return Observable
   */
  @NotNull
  private <T> Observable<T> _createErrorHandled(@NotNull Object pIdentifier, @NotNull Supplier<Observable<T>> pObservableSupplier)
  {
    // retrieve a cache that contains all previous requests
    LoadingCache<Object, Object> requests = requestCache.getUnchecked(pIdentifier);

    // insert a new request (with something unique - if an UUID is too slow, than change it to something faster)
    requests.getUnchecked(UUID.randomUUID().toString());

    // too many requests?
    if (requests.size() >= _REQUESTS_MAX_PER_TIMESLOT)
      throw new ObservableCacheRecursiveCreationException("An observable was prevented from beeing created too often " +
                                                              "(max " + _REQUESTS_MAX_PER_TIMESLOT + " items), during " + _REQUESTS_TIMESLOT + "ms");

    return pObservableSupplier.get()
        .onErrorResumeNext((Function<Throwable, ObservableSource<T>>) pEx -> {
          try
          {
            return _createErrorHandled(pIdentifier, pObservableSupplier);
          }
          catch (Exception e)
          {
            // Return the "original" error, because the current one is usually not as important as the original one (?)
            return Observable.error(pEx);
          }
        });
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
    finally
    {
      valid.set(false);
    }
  }

  /**
   * Trims the ReplaySubject, so that the current size and the max size of the buffer match each other.
   * This method is used with a ReplaySubject with a buffer size of 1.
   * The ReplaySubject should lose its value on completion.
   *
   * @param pSubject ReplaySubject to trim
   */
  private void _trimSizeBoundBuffer(@NotNull ReplaySubject<?> pSubject)
  {
    try
    {
      if (_BUFFER_FIELD == null)
      {
        _BUFFER_FIELD = ReplaySubject.class.getDeclaredField("buffer");
        _BUFFER_FIELD.setAccessible(true);
      }

      Object bufferValue = _BUFFER_FIELD.get(pSubject);
      Method trim = bufferValue.getClass().getDeclaredMethod("trim");
      trim.setAccessible(true);
      trim.invoke(bufferValue);
    }
    catch (Exception e)
    {
      throw new RuntimeException("Failed to trim replay subject buffer. " +
                                     "This may lead to memory leaks because of replaying old values", e);
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
      if (pNotification.getCause() == RemovalCause.REPLACED)
        return;

      Collection<Disposable> removedDisposables = disposableRegistry.removeAll(pNotification.getKey());
      if (removedDisposables != null)
        removedDisposables.forEach(pDisposable -> {
          if (!pDisposable.isDisposed())
            pDisposable.dispose();
        });
    }
  }
}
