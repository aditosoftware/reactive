package de.adito.util.reactive;

import io.reactivex.*;
import io.reactivex.disposables.*;
import org.jetbrains.annotations.NotNull;

import java.util.function.Consumer;

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
    LISTENER listener = registerListener(listenableValue, emitter::onNext);
    _Disposable disposable = new _Disposable(listener);
    emitter.setDisposable(disposable);
  }

  /**
   * Creates a new Listener and adds it to the VALUE.
   * If the value is changed / the listener is activated pOnNext has to be called
   *
   * @param pListenableValue Value that we're listening to
   * @param pOnNext          onNext function of the emitter to fire a new value
   * @return the strong listener, not <tt>null</tt>
   */
  @NotNull
  protected abstract LISTENER registerListener(@NotNull MODEL pListenableValue, @NotNull Consumer<VALUE> pOnNext);

  /**
   * Removes the listener, that was added by registerListener, from VALUE
   *
   * @param pListenableValue Listenable Value, to which the listener reacts
   * @param pLISTENER        Listener, that is to be removed
   */
  protected abstract void removeListener(@NotNull MODEL pListenableValue, @NotNull LISTENER pLISTENER);

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
