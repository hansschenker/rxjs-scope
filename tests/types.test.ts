import { expectTypeOf, it } from 'vitest';
import { of } from 'rxjs';
import type { MonoTypeOperatorFunction, Observable, Subscription } from 'rxjs';
import { createScope, latestValues } from '../src/index.js';
import type { Scope, ScopedOperator } from '../src/index.js';

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

it('preserves the selected value type through pipe and direct application', () => {
  const scope = createScope<number>();
  const piped$ = of(1).pipe(scope);
  const applied$ = scope(of(2));

  expectTypeOf(scope).toEqualTypeOf<ScopedOperator<number>>();
  expectTypeOf(scope).toMatchTypeOf<Scope>();
  expectTypeOf(scope).toMatchTypeOf<MonoTypeOperatorFunction<number>>();
  expectTypeOf(piped$).toEqualTypeOf<Observable<number>>();
  expectTypeOf(applied$).toEqualTypeOf<Observable<number>>();

  // @ts-expect-error A number operator cannot be applied to a string source.
  void of('incompatible').pipe(scope);
  // @ts-expect-error Direct application preserves the same source constraint.
  void scope(of('incompatible'));
  scope.dispose();
});

it('keeps connect generic even when the callable scope has a fixed value type', () => {
  const scope = createScope<number>();
  const connection = scope.connect(
    { next: value => { expectTypeOf(value).toEqualTypeOf<string>(); } },
    of('status'),
  );

  expectTypeOf(connection).toEqualTypeOf<Subscription>();
  scope.dispose();
});

it('retains source inference for no-argument method-style use', () => {
  const scope = createScope();
  expectTypeOf(scope).toEqualTypeOf<ScopedOperator<unknown>>();

  scope.connect(
    { next: value => { expectTypeOf(value).toEqualTypeOf<{ count: number }>(); } },
    of({ count: 1 }),
  );
  scope.dispose();
});
