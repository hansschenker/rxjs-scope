import { expectTypeOf, it } from 'vitest';
import { of } from 'rxjs';
import type { Observable, Subscription } from 'rxjs';
import { createScope, latestValues } from '../src/index.js';

it('infers source values in a function-first connection and exposes the lifecycle types', () => {
  const scope = createScope();
  const connection = scope.connect(
    { next: value => { expectTypeOf(value).toEqualTypeOf<number>(); } },
    of(1),
  );

  expectTypeOf(connection).toEqualTypeOf<Subscription>();
  expectTypeOf(scope.disposed$).toEqualTypeOf<Observable<void>>();
  expectTypeOf(scope.dispose).toEqualTypeOf<() => void>();
  scope.dispose();
});

it('preserves selected source values and adds only the null view marker', () => {
  const selections$ = of(of({ value: 1 }), null, undefined);
  const values$ = latestValues(selections$);

  expectTypeOf(values$).toEqualTypeOf<Observable<{ value: number } | null>>();
});
