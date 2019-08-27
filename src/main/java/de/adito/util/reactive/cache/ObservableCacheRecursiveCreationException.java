package de.adito.util.reactive.cache;

/**
 * Exception that will be thrown if the ObservableCache tries to create an observable with a recursive exception
 *
 * @author w.glanzer, 27.08.2019
 */
public class ObservableCacheRecursiveCreationException extends Exception
{

  public ObservableCacheRecursiveCreationException(String message, Throwable cause)
  {
    super(message, cause);
  }

}
