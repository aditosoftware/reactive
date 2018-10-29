package de.adito.util.reactive;

import io.reactivex.Observable;
import io.reactivex.functions.Consumer;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.*;
import org.mockito.junit.jupiter.MockitoExtension;

import java.beans.PropertyChangeListener;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

/**
 * Test for "AbstractListenerObservable"
 *
 * @author w.glanzer, 29.10.2018
 */
@ExtendWith(MockitoExtension.class)
public class Test_AbstractListenerObservable
{

  @Spy
  private _ListenerObservableImpl listenerObservable;

  private Observable<String> observable;

  @BeforeEach
  void setUp()
  {
    MockitoAnnotations.initMocks(this);
    observable = Observable.create(listenerObservable);
  }

  @Test
  void test_subscribe()
  {
    observable.subscribe();
    verify(listenerObservable, times(1)).registerListener(any(), any());
  }

  @Test
  void test_newValue() throws Exception
  {
    //noinspection unchecked
    Consumer<String> subscriber = mock(Consumer.class);

    //noinspection ResultOfMethodCallIgnored
    observable.subscribe(subscriber);

    listenerObservable.pcl.propertyChange(null);
    verify(subscriber, times(1)).accept(any());
  }

  @Test
  void test_dispose()
  {
    observable.subscribe().dispose();
    verify(listenerObservable, times(1)).removeListener(any(), any());
  }

  /**
   * AbstractListenerObservable-Impl
   */
  private static class _ListenerObservableImpl extends AbstractListenerObservable<PropertyChangeListener, Object, String>
  {
    private PropertyChangeListener pcl;

    public _ListenerObservableImpl()
    {
      super(new Object());
    }

    @NotNull
    @Override
    protected PropertyChangeListener registerListener(@NotNull Object pListenableValue, @NotNull java.util.function.Consumer<String> pOnNext)
    {
      pcl = evt -> pOnNext.accept("[new value]");
      return pcl;
    }

    @Override
    protected void removeListener(@NotNull Object pListenableValue, @NotNull PropertyChangeListener pPropertyChangeListener)
    {
      pcl = null;
    }
  }

}
