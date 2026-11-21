import { describe, expect, it } from 'vitest';
import {
  NEVER,
  Observable,
  Subject,
  defer,
  delay,
  finalize,
  of,
  repeat,
  switchMap,
} from 'rxjs';
import { TestScheduler } from 'rxjs/testing';
import { createScope } from '../src/index.js';

const trackSource = <T>() => {
  const input = new Subject<T>();
  const connections = { opened: 0, closed: 0 };
  const source$ = new Observable<T>(observer => {
    connections.opened += 1;
    const connection = input.subscribe(observer);
    return () => {
      connections.closed += 1;
      connection.unsubscribe();
    };
  });
  return { input, source$, connections };
};

describe('callable scope', () => {
  it('is lazy and shares one lifetime across pipe, direct application, and connect', () => {
    const scope = createScope<number>();
    const first = trackSource<number>();
    const second = trackSource<number>();
    const third = trackSource<string>();
    const received: (number | string)[] = [];
    const completed: string[] = [];
    const first$ = first.source$.pipe(scope);
    const second$ = scope(second.source$);
    const { connect, dispose } = scope;

    expect(first.connections.opened).toBe(0);
    expect(second.connections.opened).toBe(0);

    const a = first$.subscribe({
      next: value => received.push(value),
      complete: () => completed.push('pipe'),
    });
    const b = second$.subscribe({
      next: value => received.push(value),
      complete: () => completed.push('apply'),
    });
    const c = connect(
      {
        next: value => received.push(value),
        complete: () => completed.push('connect'),
      },
      third.source$,
    );

    first.input.next(1);
    second.input.next(2);
    third.input.next('three');
    dispose();
    dispose();
    first.input.next(99);
    second.input.next(99);
    third.input.next('late');

    expect(received).toEqual([1, 2, 'three']);
    expect(completed).toEqual(['pipe', 'apply', 'connect']);
    expect(a.closed && b.closed && c.closed).toBe(true);
    for (const source of [first, second, third]) {
      expect(source.connections).toEqual({ opened: 1, closed: 1 });
    }
  });

  it('remembers disposal for subscriptions to both old and new descriptions', () => {
    const scope = createScope<number>();
    const source = trackSource<number>();
    const beforeDisposal$ = source.source$.pipe(scope);
    let completions = 0;
    const observer = { complete: () => { completions += 1; } };

    scope.dispose();
    const first = beforeDisposal$.subscribe(observer);
    const second = source.source$.pipe(scope).subscribe(observer);
    const third = scope.connect(observer, source.source$);

    expect(source.connections).toEqual({ opened: 0, closed: 0 });
    expect(completions).toBe(3);
    expect(first.closed && second.closed && third.closed).toBe(true);
  });

  it('stops synchronous delivery when disposed before subscribe returns', () => {
    const scope = createScope<number>();
    const received: number[] = [];
    let teardowns = 0;
    const source$ = new Observable<number>(observer => {
      observer.next(1);
      observer.next(2);
      return () => { teardowns += 1; };
    });

    const connection = source$.pipe(scope).subscribe(value => {
      received.push(value);
      scope.dispose();
    });

    expect(received).toEqual([1]);
    expect(teardowns).toBe(1);
    expect(connection.closed).toBe(true);
  });

  it('keeps siblings and later connections available after natural completion', () => {
    const scope = createScope<number>();
    const sibling = trackSource<number>();
    const received: number[] = [];
    let disposalSignals = 0;
    scope.disposed$.subscribe(() => { disposalSignals += 1; });
    const siblingConnection = sibling.source$.pipe(scope).subscribe(value => received.push(value));

    const first = of(1).pipe(scope).subscribe(value => received.push(value));
    const second = of(2).pipe(scope).subscribe(value => received.push(value));
    sibling.input.next(3);

    expect(received).toEqual([1, 2, 3]);
    expect(first.closed && second.closed).toBe(true);
    expect(siblingConnection.closed).toBe(false);
    expect(disposalSignals).toBe(0);
    expect(sibling.connections).toEqual({ opened: 1, closed: 0 });

    scope.dispose();
    expect(disposalSignals).toBe(1);
    expect(sibling.connections).toEqual({ opened: 1, closed: 1 });
  });

  it('shares a lifetime while preserving independent on-demand source executions', () => {
    const scope = createScope<number>();
    const source = trackSource<number>();
    const scoped$ = source.source$.pipe(scope);
    const first = scoped$.subscribe();
    const second = scoped$.subscribe();

    expect(source.connections).toEqual({ opened: 2, closed: 0 });
    first.unsubscribe();
    expect(second.closed).toBe(false);
    expect(source.connections).toEqual({ opened: 2, closed: 1 });

    scope.dispose();
    expect(second.closed).toBe(true);
    expect(source.connections).toEqual({ opened: 2, closed: 2 });
  });

  it('keeps separate scopes independent', () => {
    const firstScope = createScope<number>();
    const secondScope = createScope<number>();
    const source = trackSource<number>();
    const first = source.source$.pipe(firstScope).subscribe();
    const second = source.source$.pipe(secondScope).subscribe();

    firstScope.dispose();
    expect(first.closed).toBe(true);
    expect(second.closed).toBe(false);
    expect(source.connections).toEqual({ opened: 2, closed: 1 });

    secondScope.dispose();
    expect(source.connections).toEqual({ opened: 2, closed: 2 });
  });

  it('cancels an active inner when the scope follows switchMap', () => {
    const scheduler = new TestScheduler((actual, expected) => {
      expect(actual).toEqual(expected);
    });

    scheduler.run(({ hot, cold, expectObservable, expectSubscriptions }) => {
      const scope = createScope<string>();
      const selections$ = hot('a--------|');
      const inner$ = cold('-x-y-z-|');
      const result$ = selections$.pipe(
        switchMap(() => inner$),
        scope,
      );

      scheduler.schedule(() => scope.dispose(), 2);
      expectObservable(result$).toBe('-x|');
      expectSubscriptions(selections$.subscriptions).toBe('^-!');
      expectSubscriptions(inner$.subscriptions).toBe('^-!');
    });
  });

  it('cancels queued delivery when the scope follows delay', () => {
    const scheduler = new TestScheduler((actual, expected) => {
      expect(actual).toEqual(expected);
    });

    scheduler.run(({ cold, expectObservable, expectSubscriptions }) => {
      const scope = createScope<string>();
      const source$ = cold('a--------|');
      const result$ = source$.pipe(delay(5, scheduler), scope);

      scheduler.schedule(() => scope.dispose(), 2);
      expectObservable(result$).toBe('--|');
      expectSubscriptions(source$.subscriptions).toBe('^-!');
    });
  });

  it('does not turn disposal into a new attempt when the scope follows repeat', () => {
    const scope = createScope<never>();
    let starts = 0;
    let teardowns = 0;
    const source$ = defer(() => {
      starts += 1;
      return NEVER.pipe(finalize(() => { teardowns += 1; }));
    });
    const connection = source$.pipe(repeat({ count: 3 }), scope).subscribe();

    scope.dispose();

    expect(connection.closed).toBe(true);
    expect(starts).toBe(1);
    expect(teardowns).toBe(1);
  });
});
