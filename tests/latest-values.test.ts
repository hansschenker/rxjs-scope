import { describe, expect, it } from 'vitest';
import { Observable, Subject, of, switchMap, takeUntil } from 'rxjs';
import { createScope, latestValues } from '../src/index.js';

describe('latestValues', () => {
  it('keeps descriptions lazy until the binding is connected', () => {
    let selectionsStarted = 0;
    let valuesStarted = 0;
    const source$ = new Observable<number>(() => {
      valuesStarted += 1;
    });
    const selections$ = new Observable<Observable<number>>(observer => {
      selectionsStarted += 1;
      observer.next(source$);
    });
    const value$ = latestValues(selections$);

    expect(selectionsStarted).toBe(0);
    expect(valuesStarted).toBe(0);

    const scope = createScope();
    scope.connect({}, value$);
    expect(selectionsStarted).toBe(1);
    expect(valuesStarted).toBe(1);
    scope.dispose();
  });

  it('reuses the same source and closes the old source before opening its replacement', () => {
    const scope = createScope();
    const selected = new Subject<Observable<number> | null>();
    const order: string[] = [];
    const received: (number | null)[] = [];
    const makeSource = (name: string, value: number) =>
      new Observable<number>(observer => {
        order.push(`open ${name}`);
        observer.next(value);
        return () => { order.push(`close ${name}`); };
      });
    const first$ = makeSource('first', 1);
    const second$ = makeSource('second', 2);

    scope.connect({ next: value => received.push(value) }, latestValues(selected));
    selected.next(first$);
    selected.next(first$);
    selected.next(second$);
    selected.next(null);

    expect(order).toEqual([
      'open first', 'close first', 'open second', 'close second',
    ]);
    expect(received).toEqual([null, 1, null, 2, null]);
    scope.dispose();
  });

  it('emits a clear marker before synchronous values as an explicit view policy', () => {
    const received: (number | null)[] = [];
    latestValues(of(of(42))).subscribe(value => received.push(value));

    expect(received).toEqual([null, 42]);
  });

  it('allows null and undefined selections without ending the selection connection', () => {
    const scope = createScope();
    const selected = new Subject<Observable<number> | null | undefined>();
    const received: (number | null)[] = [];
    const connection = scope.connect(
      { next: value => received.push(value) },
      latestValues(selected),
    );

    selected.next(null);
    selected.next(null);
    selected.next(undefined);
    selected.next(of(3));

    expect(received).toEqual([null, null, null, 3]);
    expect(connection.closed).toBe(false);
    scope.dispose();
  });

  it('accepts later selections after inner completion and does not restart an unchanged source', () => {
    const scope = createScope();
    const selected = new Subject<Observable<number> | null>();
    const received: (number | null)[] = [];
    const first$ = of(1);
    const connection = scope.connect(
      { next: value => received.push(value) },
      latestValues(selected),
    );

    selected.next(first$);
    selected.next(first$);
    selected.next(of(2));

    expect(received).toEqual([null, 1, null, 2]);
    expect(connection.closed).toBe(false);
    scope.dispose();
  });

  it('closes the selection connection and active inner when the owner ends', () => {
    const scope = createScope();
    const selected = new Subject<Observable<number>>();
    const input = new Subject<number>();
    const closed: string[] = [];
    const received: (number | null)[] = [];
    const selections$ = new Observable<Observable<number>>(observer => {
      const connection = selected.subscribe(observer);
      return () => {
        closed.push('selections');
        connection.unsubscribe();
      };
    });
    const inner$ = new Observable<number>(observer => {
      const connection = input.subscribe(observer);
      return () => {
        closed.push('inner');
        connection.unsubscribe();
      };
    });
    const connection = scope.connect(
      { next: value => received.push(value) },
      latestValues(selections$),
    );

    selected.next(inner$);
    input.next(1);
    scope.dispose();
    input.next(2);

    expect(received).toEqual([null, 1]);
    expect(closed).toHaveLength(2);
    expect(closed).toContain('selections');
    expect(closed).toContain('inner');
    expect(connection.closed).toBe(true);
  });

  it('keeps the current inner after outer completion until it finishes or the scope ends', () => {
    const scope = createScope();
    const selected = new Subject<Observable<number>>();
    const input = new Subject<number>();
    const received: (number | null)[] = [];
    let teardowns = 0;
    const inner$ = new Observable<number>(observer => {
      const connection = input.subscribe(observer);
      return () => {
        teardowns += 1;
        connection.unsubscribe();
      };
    });
    const connection = scope.connect(
      { next: value => received.push(value) },
      latestValues(selected),
    );

    selected.next(inner$);
    selected.complete();
    input.next(5);
    expect(received).toEqual([null, 5]);
    expect(connection.closed).toBe(false);
    expect(teardowns).toBe(0);

    scope.dispose();
    expect(connection.closed).toBe(true);
    expect(teardowns).toBe(1);
  });

  it('forwards an inner error and stops observing further selections', () => {
    const scope = createScope();
    const selected = new Subject<Observable<number>>();
    const received: (number | null)[] = [];
    const failure = new Error('inner failure');
    const errors: unknown[] = [];
    let selectionsClosed = 0;
    let innerClosed = 0;
    const selections$ = new Observable<Observable<number>>(observer => {
      const connection = selected.subscribe(observer);
      return () => {
        selectionsClosed += 1;
        connection.unsubscribe();
      };
    });
    const connection = scope.connect(
      {
        next: value => received.push(value),
        error: error => errors.push(error),
      },
      latestValues(selections$),
    );

    selected.next(new Observable<number>(observer => {
      observer.error(failure);
      return () => { innerClosed += 1; };
    }));
    selected.next(of(99));

    expect(received).toEqual([null]);
    expect(errors).toEqual([failure]);
    expect(connection.closed).toBe(true);
    expect(selectionsClosed).toBe(1);
    expect(innerClosed).toBe(1);
    scope.dispose();
  });

  it('demonstrates why a stop before switchMap does not cancel its active inner', () => {
    const selected = new Subject<Observable<number>>();
    const stop = new Subject<void>();
    let teardowns = 0;
    const inner$ = new Observable<number>(() => () => { teardowns += 1; });
    const connection = selected.pipe(
      takeUntil(stop),
      switchMap(source$ => source$),
    ).subscribe();

    selected.next(inner$);
    stop.next();

    expect(connection.closed).toBe(false);
    expect(teardowns).toBe(0);
    connection.unsubscribe();
    expect(teardowns).toBe(1);
  });
});
