package de.adito.util.reactive.backpressure;

import io.reactivex.rxjava3.disposables.Disposable;
import io.reactivex.rxjava3.exceptions.OnErrorNotImplementedException;
import io.reactivex.rxjava3.functions.*;
import io.reactivex.rxjava3.plugins.RxJavaPlugins;
import org.reactivestreams.*;

import java.util.concurrent.atomic.AtomicReference;

/**
 * FlowableSubscriber that is able to apply backpressure.
 * Is loosely based on io.reactivex.rxjava3.subscribers.DisposableSubscriber, but does not immediately request the max number of items.
 * Instead, only one item is requested at the start, and another one after each onNext call
 *
 * @author m.kaspera, 30.07.2020
 */
public class BackpressureSubscriber<T> implements Disposable, Subscriber<T>
{
  final AtomicReference<Subscription> upstream = new AtomicReference<>();
  private static final Consumer<Throwable> ON_ERROR_DEFAULT = pThrowable -> RxJavaPlugins.onError(new OnErrorNotImplementedException(pThrowable));
  private static final Action EMPTY_ACTION = () -> {
  };
  private final Consumer<T> onNextFn;
  private final Consumer<Throwable> onErrorFn;
  private final Action onCompleteFn;

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

    onNextFn = pOnNext;
    onErrorFn = pOnError;
    onCompleteFn = pOnComplete;
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
    upstream.get().request(1);
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
    upstream.get().request(1);
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
}
