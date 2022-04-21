package de.adito.util.reactive.scheduler;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.reactivex.rxjava3.core.Scheduler;
import io.reactivex.rxjava3.schedulers.Schedulers;
import org.jetbrains.annotations.NotNull;

import java.util.concurrent.*;
import java.util.function.Function;
import java.util.logging.Logger;

/**
 * This Scheduler is backed up by a cached threadpool.
 * It will reuse old threads, so that the pool won't grow up too large.
 *
 * Difference to Schedulers.computation(): The computation threadpool is a fixed size pool - this one isn't.
 * Difference to Schedulers.io(): IO pool spawns threads on each event, threads won't get reused - this on does
 *
 * @author w.glanzer, 12.04.2022
 */
public class CachedScheduler
{

  private static final String _THREADPOOL_NAME = "tCachedRxScheduler-%d";
  private static final int _MAX_POOL_SIZE = Integer.MAX_VALUE;

  private static Scheduler _INSTANCE;
  private static Function<Scheduler, Scheduler> _HANDLER;

  /**
   * Returns the static instance of this scheduler.
   * Will be created, if not already existing.
   *
   * @see CachedScheduler
   * @return the scheduler
   */
  @NotNull
  public static Scheduler getInstance()
  {
    if(_INSTANCE == null)
    {
      ThreadPoolExecutor executor = new ThreadPoolExecutor(Runtime.getRuntime().availableProcessors(), _MAX_POOL_SIZE, 60, TimeUnit.SECONDS,
                                                           new SynchronousQueue<>());
      executor.allowCoreThreadTimeOut(true);
      executor.setThreadFactory(new ThreadFactoryBuilder()
                                    .setDaemon(true)
                                    .setNameFormat(_THREADPOOL_NAME)
                                    .build());

      Scheduler scheduler = Schedulers.from(executor);
      if(_HANDLER != null)
        scheduler = _HANDLER.apply(scheduler);

      _INSTANCE = scheduler;
    }

    return _INSTANCE;
  }

  /**
   * Sets the scheduler handler to modify it on creation
   *
   * @param pFn Function that will modify this scheduler
   */
  public static void setSchedulerHandler(@NotNull Function<Scheduler, Scheduler> pFn)
  {
    if(_INSTANCE == null)
      Logger.getLogger(CachedScheduler.class.getName()).warning("Setting the scheduler handler after instance creation is currently not supported");
    _HANDLER = pFn;
  }

}
