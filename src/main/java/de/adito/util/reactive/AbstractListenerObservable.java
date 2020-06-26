package de.adito.util.reactive;

import io.reactivex.rxjava3.core.*;
import io.reactivex.rxjava3.disposables.Disposable;
import org.jetbrains.annotations.NotNull;

/**
 * Abstract implementation of an RxJava Observable.
 * This abstraction connects the normal listeners with the reactive programming paradigm.
 *
 * @author w.glanzer, 23.04.2018
 */
public abstract class AbstractListenerObservable<LISTENER, MODEL, VALUE> implements ObservableOnSubscribe<VALUE>
{
  private MODEL listenableValue;

  public AbstractListenerObservable(@NotNull MODEL pListenableValue)
  {
    listenableValue = pListenableValue;
  }

  @Override
  public final void subscribe(ObservableEmitter<VALUE> emitter)
  {
    LISTENER listener = registerListener(listenableValue, new _EmitterFireable<>(emitter));
    _Disposable disposable = new _Disposable(listener);
    emitter.setDisposable(disposable);
  }

  /**
   * Creates a new Listener and adds it to the VALUE.
   * If the value is changed / the listener is activated pOnNext has to be called
   *
   * @param pListenableValue Value that we're listening to
   * @param pFireable        Interface to publish new values
   * @return the strong listener, not <tt>null</tt>
   */
  @NotNull
  protected abstract LISTENER registerListener(@NotNull MODEL pListenableValue, @NotNull IFireable<VALUE> pFireable);

  /**
   * Removes the listener, that was added by registerListener, from VALUE
   *
   * @param pListenableValue Listenable Value, to which the listener reacts
   * @param pLISTENER        Listener, that is to be removed
   */
  protected abstract void removeListener(@NotNull MODEL pListenableValue, @NotNull LISTENER pLISTENER);

  /**
   * Interface to publish new values or completion
   */
  public interface IFireable<VALUE>
  {
    /**
     * Fire that the value of this Observable has changed
     *
     * @param pNewValue new value, not <tt>null</tt>
     */
    void fireValueChanged(@NotNull VALUE pNewValue);

    /**
     * Fires, that the listener failed execution
     *
     * @param pError Failure
     */
    void fireListenerFailure(@NotNull Throwable pError);

    /**
     * Fires, that the observable completed and won't fire any more value changes
     */
    void fireCompleted();
  }

  /**
   * Fireable-Wrapper for an ObservableEmitter
   */
  private static class _EmitterFireable<VALUE> implements IFireable<VALUE>
  {
    private final Emitter<VALUE> emitter;

    public _EmitterFireable(@NotNull Emitter<VALUE> pEmitter)
    {
      emitter = pEmitter;
    }

    @Override
    public void fireValueChanged(@NotNull VALUE pNewValue)
    {
      emitter.onNext(pNewValue);
    }

    @Override
    public void fireListenerFailure(@NotNull Throwable pError)
    {
      emitter.onError(pError);
    }

    @Override
    public void fireCompleted()
    {
      emitter.onComplete();
    }
  }

  /**
   * Disposable-Impl, that keeps the listener
   */
  private class _Disposable implements Disposable
  {
    private LISTENER listener;

    _Disposable(LISTENER pListener)
    {
      listener = pListener;
    }

    @Override
    public void dispose()
    {
      if (!isDisposed() && listener != null)
      {
        removeListener(listenableValue, listener);
        listener = null;
      }
    }

    @Override
    public boolean isDisposed()
    {
      return listener == null;
    }
  }

}
