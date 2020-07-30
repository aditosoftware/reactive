package de.adito.util.reactive.backpressure;

import io.reactivex.rxjava3.core.Flowable;

import java.io.*;

/**
 * Class that makes it easy to create a Flowable from a reader
 *
 * @author m.kaspera, 30.07.2020
 */
public class FlowableFromReader
{

  /**
   * Create a new Flowable from the BufferedReader. The onNext calls each repesent a full line
   *
   * @param pReader BufferedReader
   * @return Flowable that reads lines from the BufferedReader and calls onNext for each line
   */
  public static Flowable<String> create(BufferedReader pReader)
  {
    return Flowable.generate(() -> pReader, (bReader, emitter) -> {
      try
      {
        String readLine = bReader.readLine();
        if (readLine != null)
          emitter.onNext(readLine);
        else
        {
          // readLine == null -> The reader is done reading -> end the flowable
          emitter.onComplete();
        }
      }
      catch (IOException pIOException)
      {
        emitter.onError(pIOException);
      }
    });
  }

}
