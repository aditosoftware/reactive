package de.adito.util.reactive;

import io.reactivex.Observable;
import io.reactivex.disposables.Disposable;
import io.reactivex.functions.Consumer;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.*;
import org.mockito.junit.jupiter.MockitoExtension;

import java.beans.*;
import java.util.*;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

/**
 * Test for "AbstractListenerObservable"
 *
 * @author w.glanzer, 29.10.2018
 */
@SuppressWarnings({"ResultOfMethodCallIgnored", "unchecked"})
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
    Consumer<String> subscriber = mock(Consumer.class);
    observable.subscribe(subscriber);
    listenerObservable._fire();
    verify(subscriber, times(1)).accept(any());
  }

  @Test
  void test_dispose()
  {
    observable.subscribe().dispose();
    verify(listenerObservable, times(1)).removeListener(any(), any());
  }

  @Test
  void test_multi_subscribe()
  {
    observable.subscribe();
    observable.subscribe();
    observable.subscribe();

    verify(listenerObservable, times(3)).registerListener(any(), any());
  }

  @Test
  void test_multi_newValue() throws Exception
  {
    Consumer<String> subscriber = mock(Consumer.class);
    Consumer<String> subscriber2 = mock(Consumer.class);
    Consumer<String> subscriber3 = mock(Consumer.class);

    observable.subscribe(subscriber);
    observable.subscribe(subscriber2);
    observable.subscribe(subscriber3);

    listenerObservable._fire();

    verify(subscriber, times(1)).accept(any());
    verify(subscriber2, times(1)).accept(any());
    verify(subscriber3, times(1)).accept(any());
  }

  @Test
  void test_multi_newValueAndDispose() throws Exception
  {
    Consumer<String> subscriber = mock(Consumer.class);
    Consumer<String> subscriber2 = mock(Consumer.class);
    Consumer<String> subscriber3 = mock(Consumer.class);
    Consumer<String> subscriber4 = mock(Consumer.class);

    Disposable disp1 = observable.subscribe(subscriber);
    Disposable disp2 = observable.subscribe(subscriber2);
    observable.subscribe(subscriber3);
    observable.subscribe(subscriber4);

    listenerObservable._fire();
    verify(subscriber, times(1)).accept(any());
    verify(subscriber2, times(1)).accept(any());
    verify(subscriber3, times(1)).accept(any());
    verify(subscriber4, times(1)).accept(any());

    // Dipose 1, must not fire anymore
    disp1.dispose();
    listenerObservable._fire();
    verify(subscriber, times(1)).accept(any());
    verify(subscriber2, times(2)).accept(any());
    verify(subscriber3, times(2)).accept(any());
    verify(subscriber4, times(2)).accept(any());

    // Dipose 2, must not fire anymore
    disp2.dispose();
    listenerObservable._fire();
    verify(subscriber, times(1)).accept(any());
    verify(subscriber2, times(2)).accept(any());
    verify(subscriber3, times(3)).accept(any());
    verify(subscriber4, times(3)).accept(any());
  }

  @Test
  void test_multi_dispose()
  {
    observable.subscribe().dispose();
    observable.subscribe().dispose();
    observable.subscribe().dispose();

    verify(listenerObservable, times(3)).removeListener(any(), any());
  }

  @Test
  void test_removeListenerAfterCompletion()
  {
    Disposable disp = observable.subscribe();

    // Fire some changes
    listenerObservable._fire("one");
    listenerObservable._fire("two");
    verify(listenerObservable, times(0)).removeListener(any(), any());

    // Fire closing event
    listenerObservable._fire("close");
    Assertions.assertTrue(disp.isDisposed());
    verify(listenerObservable, times(1)).removeListener(any(), any());
  }

  /**
   * AbstractListenerObservable-Impl
   */
  private static class _ListenerObservableImpl extends AbstractListenerObservable<PropertyChangeListener, Object, String>
  {
    private Set<PropertyChangeListener> pcl = Collections.synchronizedSet(new HashSet<>());

    public _ListenerObservableImpl()
    {
      super(new Object());
    }

    @NotNull
    @Override
    protected PropertyChangeListener registerListener(@NotNull Object pListenableValue, @NotNull IFireable pFireable)
    {
      PropertyChangeListener pc = evt -> {
        if(Objects.equals(evt.getPropertyName(), "close"))
          pFireable.fireCompleted();
        else
          pFireable.fireValueChanged("[new value]");
      };
      pcl.add(pc);
      return pc;
    }

    @Override
    protected void removeListener(@NotNull Object pListenableValue, @NotNull PropertyChangeListener pPropertyChangeListener)
    {
      pcl.remove(pPropertyChangeListener);
    }

    private void _fire()
    {
      _fire("");
    }

    private void _fire(String pProp)
    {
      pcl.forEach(pL -> pL.propertyChange(new PropertyChangeEvent(this, pProp, null, null)));
    }
  }

}
