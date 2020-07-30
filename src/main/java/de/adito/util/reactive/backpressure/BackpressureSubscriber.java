package de.adito.util.reactive.backpressure;

import io.reactivex.rxjava3.core.FlowableSubscriber;
import io.reactivex.rxjava3.disposables.Disposable;
import io.reactivex.rxjava3.functions.*;
import io.reactivex.rxjava3.internal.functions.Functions;
import io.reactivex.rxjava3.internal.subscriptions.SubscriptionHelper;
import io.reactivex.rxjava3.internal.util.EndConsumerHelper;
import io.reactivex.rxjava3.plugins.RxJavaPlugins;
import org.reactivestreams.Subscription;

import java.util.concurrent.atomic.AtomicReference;

/**
 * FlowableSubscriber that is able to apply backpressure.
 * Is based on io.reactivex.rxjava3.subscribers.DisposableSubscriber, but does not immediately request the max number of items.
 * Instead, only one item is requested at the start, and another one after each onNext call
 *
 * @author m.kaspera, 30.07.2020
 */
public class BackpressureSubscriber<T> implements FlowableSubscriber<T>, Disposable
{
  final AtomicReference<Subscription> upstream = new AtomicReference<>();
  private final Consumer<T> onNextFn;
  private final Consumer<Throwable> onErrorFn;
  private final Action onCompleteFn;

  /**
   * @param pOnNext the Consumer<T> you have designed to accept emissions from the ObservableSource
   */
  public BackpressureSubscriber(Consumer<T> pOnNext)
  {
    // if the user does not pass an error handling or complete function, use the default rxJava functions for these cases
    this(pOnNext, Functions.ON_ERROR_MISSING, Functions.EMPTY_ACTION);
  }

  /**
   * @param pOnNext  the Consumer<T> you have designed to accept emissions from the ObservableSource
   * @param pOnError the Consumer<Throwable> you have designed to accept any error notification from the ObservableSource
   */
  public BackpressureSubscriber(Consumer<T> pOnNext, Consumer<Throwable> pOnError)
  {
    this(pOnNext, pOnError, Functions.EMPTY_ACTION);
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
    request(1);
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

  // ------------------------------------------------------------------------------------------------------------------------------------------------
  // -------------------------- original code from io.reactivex.rxjava3.subscribers.DisposableSubscriber --------------------------------------------
  // ------------------------------------------------------------------------------------------------------------------------------------------------


  @Override
  public final void onSubscribe(Subscription s)
  {
    if (EndConsumerHelper.setOnce(this.upstream, s, getClass()))
    {
      onStart();
    }
  }

  /**
   * Called once the single upstream {@link Subscription} is set via {@link #onSubscribe(Subscription)}.
   */
  protected void onStart()
  {
    upstream.get().request(1);
  }

  /**
   * Requests the specified amount from the upstream if its {@link Subscription} is set via
   * onSubscribe already.
   * <p>Note that calling this method before a {@link Subscription} is set via {@link #onSubscribe(Subscription)}
   * leads to {@link NullPointerException} and meant to be called from inside {@link #onStart()} or
   * {@link #onNext(Object)}.
   *
   * @param n the request amount, positive
   */
  protected final void request(long n)
  {
    upstream.get().request(n);
  }

  /**
   * Cancels the Subscription set via {@link #onSubscribe(Subscription)} or makes sure a
   * {@link Subscription} set asynchronously (later) is cancelled immediately.
   * <p>This method is thread-safe and can be exposed as a public API.
   */
  protected final void cancel()
  {
    dispose();
  }

  @Override
  public final boolean isDisposed()
  {
    return upstream.get() == SubscriptionHelper.CANCELLED;
  }

  @Override
  public final void dispose()
  {
    SubscriptionHelper.cancel(upstream);
  }
}
