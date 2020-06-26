package de.adito.util.reactive;

import io.reactivex.rxjava3.core.Observable;
import org.junit.jupiter.api.*;

import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author w.glanzer, 03.12.2018
 */
public class Test_Observables
{

  @Test
  void test_create()
  {
    AtomicBoolean created = new AtomicBoolean(false);
    Observable<String> observable = Observables.create(emitter -> emitter.onNext("subscribed"), () -> {
      // This must be called after our chain is created
      Assertions.assertTrue(created.get());
      return "default";
    });

    // Now set the created-flag to TRUE, because we created our observable-chain successfully
    created.set(true);

    // read all values and assert the correct order
    List<String> values = new ArrayList<>();
    observable.subscribe(values::add).dispose();
    Assertions.assertEquals(Arrays.asList("default", "subscribed"), values);
  }

}