package de.adito.util.reactive.backpressure;

import io.reactivex.rxjava3.disposables.Disposable;
import io.reactivex.rxjava3.exceptions.OnErrorNotImplementedException;
import io.reactivex.rxjava3.functions.*;
import io.reactivex.rxjava3.plugins.RxJavaPlugins;
import org.reactivestreams.*;

import java.util.concurrent.atomic.*;

/**
 * FlowableSubscriber that is able to apply backpressure.
 * Is loosely based on io.reactivex.rxjava3.subscribers.DisposableSubscriber, but does not immediately request the max number of items.
 * Instead, only one item is requested at the start, and another one after each onNext call
 *
 * @author m.kaspera, 30.07.2020
 */
public class BackpressureSubscriber<T> implements Disposable, Subscriber<T>
{
  private static final Integer DEFAULT_REQUESTS_INITIAL_SIZE = 256;
  private static final Integer DEFAULT_REQUESTS_BATCH_SIZE = 64;
  private static final Consumer<Throwable> ON_ERROR_DEFAULT = pThrowable -> RxJavaPlugins.onError(new OnErrorNotImplementedException(pThrowable));
  private static final Action EMPTY_ACTION = () -> {
  };
  private final AtomicReference<Subscription> upstream = new AtomicReference<>();
  private final AtomicInteger counter = new AtomicInteger(0);
  private final Consumer<T> onNextFn;
  private final Consumer<Throwable> onErrorFn;
  private final Action onCompleteFn;
  private final int requestInititalSize;
  private final int requestBatchSize;

  /**
   * @param pOnNext the Consumer<T> you have designed to accept emissions from the ObservableSource
   */
  public BackpressureSubscriber(Consumer<T> pOnNext)
  {
    // if the user does not pass an error handling or complete function, use the default rxJava functions for these cases
    this(pOnNext, ON_ERROR_DEFAULT, EMPTY_ACTION);
  }

  /**
   * @param pOnNext  the Consumer<T> you have designed to accept emissions from the ObservableSource
   * @param pOnError the Consumer<Throwable> you have designed to accept any error notification from the ObservableSource
   */
  public BackpressureSubscriber(Consumer<T> pOnNext, Consumer<Throwable> pOnError)
  {
    this(pOnNext, pOnError, EMPTY_ACTION);
  }

  /**
   * @param pOnNext     the Consumer<T> you have designed to accept emissions from the ObservableSource
   * @param pOnError    the Consumer<Throwable> you have designed to accept any error notification from the ObservableSource
   * @param pOnComplete the Action you have designed to accept a completion notification from the ObservableSource
   */
  public BackpressureSubscriber(Consumer<T> pOnNext, Consumer<Throwable> pOnError, Action pOnComplete)
  {

    this(pOnNext, pOnError, pOnComplete, DEFAULT_REQUESTS_INITIAL_SIZE, DEFAULT_REQUESTS_BATCH_SIZE);
  }

  /**
   * @param pOnNext              the Consumer<T> you have designed to accept emissions from the ObservableSource
   * @param pOnError             the Consumer<Throwable> you have designed to accept any error notification from the ObservableSource
   * @param pOnComplete          the Action you have designed to accept a completion notification from the ObservableSource
   * @param pRequestInititalSize Number of initial items that are requested for the backpressure mechanism
   * @param pRequestBatchSize    Number, after which a new set of requests is requested (e.g. for 64, after 64 onNext calls 64 new items are requested)
   */
  private BackpressureSubscriber(Consumer<T> pOnNext, Consumer<Throwable> pOnError, Action pOnComplete,
                                 int pRequestInititalSize, int pRequestBatchSize)
  {
    onNextFn = pOnNext;
    onErrorFn = pOnError;
    onCompleteFn = pOnComplete;
    requestInititalSize = pRequestInititalSize;
    requestBatchSize = pRequestBatchSize;
  }

  @Override
  public void onNext(T pT)
  {
    try
    {
      onNextFn.accept(pT);
    }
    catch (Throwable pThrowable)
    {
      onError(pThrowable);
    }
    if (counter.incrementAndGet() % requestBatchSize == 0)
      upstream.get().request(requestBatchSize);
  }

  @Override
  public void onError(Throwable pThrowable)
  {
    try
    {
      onErrorFn.accept(pThrowable);
    }
    catch (Throwable pE)
    {
      RxJavaPlugins.onError(pThrowable);
    }
  }

  @Override
  public void onComplete()
  {
    try
    {
      onCompleteFn.run();
    }
    catch (Throwable pThrowable)
    {
      onError(pThrowable);
    }
  }

  @Override
  public final void onSubscribe(Subscription s)
  {
    // set the subscription as upstream and request the first item
    upstream.set(s);
    upstream.get().request(requestInititalSize);
  }

  @Override
  public final boolean isDisposed()
  {
    // if no subscription is available, the subscriber has been disposed
    return upstream.get() == null;
  }

  @Override
  public final void dispose()
  {
    // cancel the subscription and set upstream to null
    upstream.get().cancel();
    upstream.set(null);
  }

  /**
   * Builder for creating BackpressureSubscribersin a comfortable manner
   * All the essential parameters have to be passed in the constructor, every other parameter uses a default value if it is not set explicitly
   *
   * @param <T> The kind of object that the BackpressureSubscriber will emit
   */
  public static class Builder<T>
  {
    private final Consumer<T> onNextFn;
    private Consumer<Throwable> onErrorFn = ON_ERROR_DEFAULT;
    private Action onCompleteFn = EMPTY_ACTION;
    private int requestBatchSize = DEFAULT_REQUESTS_BATCH_SIZE;
    private int requestInititalSize = DEFAULT_REQUESTS_INITIAL_SIZE;

    public Builder(Consumer<T> pOnNextFn)
    {
      onNextFn = pOnNextFn;
    }

    /**
     * Defaults to the "OnErrorNotImplementedException" being thrown by the RxJava Framework
     *
     * @param pErrorFn the Consumer<Throwable> you have designed to accept any error notification from the ObservableSource
     * @return this builder
     */
    public Builder<T> setOnErrorFn(Consumer<Throwable> pErrorFn)
    {
      onErrorFn = pErrorFn;
      return this;
    }

    /**
     * @param pOnCompleteFn the Action you have designed to accept a completion notification from the ObservableSource
     * @return this builder
     */
    public Builder<T> setOnCompleteFn(Action pOnCompleteFn)
    {
      onCompleteFn = pOnCompleteFn;
      return this;
    }

    /**
     *
     * @param pInitialRequests Number of initial items that are requested for the backpressure mechanism
     * @return this builder
     */
    public Builder<T> setInitialRequests(int pInitialRequests)
    {
      requestInititalSize = pInitialRequests;
      return this;
    }

    /**
     *
     * @param pRequestBatchSize Number, after which a new set of requests is requested (e.g. for 64, after 64 onNext calls 64 new items are requested)
     * @return this builder
     */
    public Builder<T> setRequestsBatchSize(int pRequestBatchSize)
    {
      requestBatchSize = pRequestBatchSize;
      return this;
    }

    /**
     * creates the BackpressureSubscriber with the paremeters set so far (all non-set parameters use their default values)
     *
     * @return the BackpressureSubscriber
     */
    public BackpressureSubscriber<T> create()
    {
      return new BackpressureSubscriber<>(onNextFn, onErrorFn, onCompleteFn, requestInititalSize, requestBatchSize);
    }
  }
}
