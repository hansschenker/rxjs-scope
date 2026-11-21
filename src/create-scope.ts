import { ReplaySubject, takeUntil } from 'rxjs';
import type { Observable, Observer, Subscription } from 'rxjs';

/** One owner's lifetime and the connections attached to it. */
export type Scope = Readonly<{
  /** Emits once on disposal and remembers that event for late observers. */
  disposed$: Observable<void>;

  /**
   * Connect at the effect boundary. The lifetime gate is downstream of the
   * supplied pipeline, including its flattening and sharing operators.
   * A disposed scope completes the observer without starting the source.
   */
  connect: <T>(
    observer: Partial<Observer<T>>,
    source$: Observable<T>,
  ) => Subscription;

  /** End all remaining connections. Repeated calls have no further effect. */
  dispose: () => void;
}>;

/**
 * Create a scope for a view, route, animation, or task.
 *
 * Creation does not connect to any source. The owner must invoke `dispose`
 * at its real lifetime boundary. RxJS does not infer DOM removal.
 *
 * Lifetime cancellation completes downstream observers and unsubscribes
 * upstream. It does not mean that an underlying operation finished normally.
 */
export const createScope = (): Scope => {
  const disposed = new ReplaySubject<void>(1);
  const disposed$ = disposed.asObservable();
  let isDisposed = false;

  const connect = <T>(
    observer: Partial<Observer<T>>,
    source$: Observable<T>,
  ): Subscription => source$.pipe(takeUntil(disposed$)).subscribe(observer);

  const dispose = (): void => {
    if (isDisposed) return;

    isDisposed = true;
    disposed.next();
    disposed.complete();
  };

  return { disposed$, connect, dispose };
};
