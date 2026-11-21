import { describe, expect, it } from 'vitest';
import {
  Observable,
  Subject,
  of,
  shareReplay,
  takeUntil,
} from 'rxjs';
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

describe('createScope', () => {
  it('starts each connection on demand and closes all owned connections once', () => {
    const scope = createScope();
    const first = trackSource<number>();
    const second = trackSource<number>();
    const received: number[] = [];
    const completed: string[] = [];

    expect(first.connections.opened).toBe(0);
    expect(second.connections.opened).toBe(0);

    const firstConnection = scope.connect(
      {
        next: value => received.push(value),
        complete: () => completed.push('first'),
      },
      first.source$,
    );
    const secondConnection = scope.connect(
      { complete: () => completed.push('second') },
      second.source$,
    );

    first.input.next(1);
    scope.dispose();
    scope.dispose();
    first.input.next(2);

    expect(received).toEqual([1]);
    expect(completed).toEqual(['first', 'second']);
    expect(firstConnection.closed).toBe(true);
    expect(secondConnection.closed).toBe(true);
    expect(first.connections).toEqual({ opened: 1, closed: 1 });
    expect(second.connections).toEqual({ opened: 1, closed: 1 });
  });

  it('rejects a late connection without starting its source', () => {
    const scope = createScope();
    const source = trackSource<number>();
    let completions = 0;

    scope.dispose();
    const connection = scope.connect(
      { complete: () => { completions += 1; } },
      source.source$,
    );

    expect(connection.closed).toBe(true);
    expect(completions).toBe(1);
    expect(source.connections).toEqual({ opened: 0, closed: 0 });
  });

  it('also rejects a connection attempted during disposal', () => {
    const scope = createScope();
    const first = trackSource<number>();
    const late = trackSource<number>();
    let lateClosed = false;

    scope.connect(
      {
        complete: () => {
          lateClosed = scope.connect({}, late.source$).closed;
        },
      },
      first.source$,
    );
    scope.dispose();

    expect(lateClosed).toBe(true);
    expect(late.connections).toEqual({ opened: 0, closed: 0 });
    expect(first.connections.closed).toBe(1);
  });

  it('handles disposal during a synchronous emission before subscribe returns', () => {
    const scope = createScope();
    const received: number[] = [];
    let teardowns = 0;
    const source$ = new Observable<number>(observer => {
      observer.next(1);
      observer.next(2);

      return () => { teardowns += 1; };
    });

    const connection = scope.connect(
      {
        next: value => {
          received.push(value);
          scope.dispose();
        },
      },
      source$,
    );

    expect(received).toEqual([1]);
    expect(connection.closed).toBe(true);
    expect(teardowns).toBe(1);
  });

  it('lets one connection finish naturally while the owner remains available', () => {
    const scope = createScope();
    const received: number[] = [];
    const first = scope.connect({ next: value => received.push(value) }, of(1));
    const second = scope.connect({ next: value => received.push(value) }, of(2));

    expect(received).toEqual([1, 2]);
    expect(first.closed).toBe(true);
    expect(second.closed).toBe(true);
    scope.dispose();
  });

  it('forwards a source error without disposing sibling connections', () => {
    const scope = createScope();
    const sibling = trackSource<number>();
    const received: number[] = [];
    const failure = new Error('source failure');
    const failures: unknown[] = [];
    let teardowns = 0;
    const siblingConnection = scope.connect(
      { next: value => received.push(value) },
      sibling.source$,
    );
    const failedConnection = scope.connect(
      { error: error => failures.push(error) },
      new Observable<never>(observer => {
        observer.error(failure);
        return () => { teardowns += 1; };
      }),
    );

    sibling.input.next(7);
    expect(failures).toEqual([failure]);
    expect(failedConnection.closed).toBe(true);
    expect(teardowns).toBe(1);
    expect(siblingConnection.closed).toBe(false);
    expect(received).toEqual([7]);
    scope.dispose();
  });

  it('supports closing one connection early without completing it or ending siblings', () => {
    const scope = createScope();
    const source = trackSource<number>();
    let completions = 0;
    const first = scope.connect(
      { complete: () => { completions += 1; } },
      source.source$,
    );
    const second = scope.connect({}, source.source$);

    first.unsubscribe();
    expect(completions).toBe(0);
    expect(second.closed).toBe(false);
    expect(source.connections).toEqual({ opened: 2, closed: 1 });

    scope.dispose();
    expect(source.connections).toEqual({ opened: 2, closed: 2 });
  });

  it('announces disposal once and remembers it for later lifetime observers', () => {
    const scope = createScope();
    const initial: string[] = [];
    const later: string[] = [];
    scope.disposed$.subscribe({
      next: () => initial.push('disposed'),
      complete: () => initial.push('complete'),
    });

    scope.dispose();
    scope.dispose();
    scope.disposed$.subscribe({
      next: () => later.push('disposed'),
      complete: () => later.push('complete'),
    });

    expect(initial).toEqual(['disposed', 'complete']);
    expect(later).toEqual(['disposed', 'complete']);
  });

  it('shares only when the supplied pipeline explicitly shares', () => {
    const scope = createScope();
    const source = trackSource<number>();
    const shared$ = source.source$.pipe(
      shareReplay({ bufferSize: 1, refCount: true }),
    );
    const first = scope.connect({}, shared$);
    const second = scope.connect({}, shared$);

    expect(source.connections.opened).toBe(1);
    first.unsubscribe();
    expect(source.connections.closed).toBe(0);
    scope.dispose();

    expect(second.closed).toBe(true);
    expect(source.connections).toEqual({ opened: 1, closed: 1 });
  });

  it('leaves a shared producer running for consumers outside the disposed scope', () => {
    const scope = createScope();
    const source = trackSource<number>();
    const shared$ = source.source$.pipe(
      shareReplay({ bufferSize: 1, refCount: true }),
    );
    const outsideValues: number[] = [];
    const outside = shared$.subscribe(value => outsideValues.push(value));
    scope.connect({}, shared$);

    scope.dispose();
    source.input.next(9);
    expect(outsideValues).toEqual([9]);
    expect(source.connections.closed).toBe(0);

    outside.unsubscribe();
    expect(source.connections.closed).toBe(1);
  });

  it('blocks completed replay values when connecting through a disposed scope', () => {
    const scope = createScope();
    const shared$ = of(42).pipe(
      shareReplay({ bufferSize: 1, refCount: true }),
    );
    const before: number[] = [];
    const after: number[] = [];
    scope.connect({ next: value => before.push(value) }, shared$);
    scope.dispose();
    scope.connect({ next: value => after.push(value) }, shared$);

    expect(before).toEqual([42]);
    expect(after).toEqual([]);
  });

  it('demonstrates that completing a takeUntil notifier alone does not stop work', () => {
    const source = trackSource<number>();
    const stop = new Subject<void>();
    const connection = source.source$.pipe(takeUntil(stop)).subscribe();

    stop.complete();
    expect(connection.closed).toBe(false);
    expect(source.connections.closed).toBe(0);
    connection.unsubscribe();
    expect(source.connections.closed).toBe(1);
  });
});
