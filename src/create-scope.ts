import { takeUntil } from 'rxjs';
import type { MonoTypeOperatorFunction } from 'rxjs';
import { createScopeCore } from './create-scope-core.js';
import type { Scope } from './create-scope-core.js';

export type { Scope } from './create-scope-core.js';

/** A lifetime handle that also works as a type-preserving RxJS operator. */
export type ScopedOperator<T> = Scope & MonoTypeOperatorFunction<T>;

/**
 * Create one scope for a view, route, animation, or task.
 *
 * Use `source$.pipe(scope)` or `scope(source$)` to describe a connection bound
 * to this lifetime, then subscribe to start it. Put the scope after operators
 * that create inner connections, schedule values, resubscribe, or share a cache
 * whose consumer lifetime it should control. Applying the operator does not
 * create another scope or share source execution.
 *
 * `T` is the value type at the operator position. `scope.connect` remains
 * independently generic, including when called on plain `createScope()`.
 *
 * The mounting runtime must invoke `dispose` at the real lifetime boundary.
 * RxJS does not infer DOM removal. A disposed scope cannot be reused; create
 * a new scope for a new mount.
 *
 * Lifetime cancellation completes downstream observers and unsubscribes
 * upstream. It does not mean that an underlying operation finished normally.
 */
export const createScope = <T = unknown>(): ScopedOperator<T> => {
  const scope = createScopeCore();
  const applyScope: MonoTypeOperatorFunction<T> = source$ =>
    source$.pipe(takeUntil(scope.disposed$));

  return Object.assign(applyScope, scope);
};
