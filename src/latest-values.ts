import { distinctUntilChanged, of, startWith, switchMap } from 'rxjs';
import type { Observable } from 'rxjs';

/** A selected source description, or an explicit selection of no source. */
export type SourceSelection<T> = Observable<T> | null | undefined;

/**
 * Follow the currently selected source using reference identity.
 *
 * A changed selection closes the previous source. `null` and `undefined`
 * disconnect it and emit `null`; selecting a source emits `null` before its
 * values. This explicit clear-view policy also applies to synchronous sources.
 *
 * This function is lazy and adds no sharing or owner lifetime by itself.
 * Connect its output through a scope. An unhandled source error terminates
 * the binding; inner completion still permits later selections.
 */
export const latestValues = <T>(
  sources$: Observable<SourceSelection<T>>,
): Observable<T | null> =>
  sources$.pipe(
    distinctUntilChanged(),
    switchMap(source$ =>
      source$ == null ? of(null) : source$.pipe(startWith(null)),
    ),
  );
