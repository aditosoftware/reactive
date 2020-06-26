package de.adito.util.reactive;

import io.reactivex.rxjava3.core.*;
import org.jetbrains.annotations.NotNull;

import java.util.function.Supplier;

/**
 * @author w.glanzer, 03.12.2018
 */
public class Observables
{

  private Observables()
  {
  }

  /**
   * Create an Observable with a default value
   *
   * @param pObservableBlueprint Observable to build
   * @param pDefaultValue        Supplier for the default value. <b>It must not return null!</b>.
   *                             It will be called if a new observer subscribes
   * @return the created observable
   */
  @NotNull
  public static <T> Observable<T> create(@NotNull ObservableOnSubscribe<T> pObservableBlueprint, @NotNull Supplier<T> pDefaultValue)
  {
    // Get StackTrace to increase readability of the exception
    Exception stackTrace = new Exception();

    // construct observable
    return Observable.create(pObservableBlueprint)
        .startWith(_emitValue(pDefaultValue, stackTrace));
  }

  @NotNull
  private static <T> ObservableSource<T> _emitValue(@NotNull Supplier<T> pSupplier, @NotNull Exception pStackTrace)
  {
    return Observable.just(pSupplier)
        .map(pSup -> {
          T value = pSup.get();
          if (value == null)
            throw new RuntimeException("The supplier for the default value must not return null! " +
                                           "See the cause-trace for more information about the 'create()'-call in which the given supplier returned null.", pStackTrace);
          return value;
        });
  }

}
