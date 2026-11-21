import { fromEvent, interval, map, merge, of } from 'rxjs';
import { createScope, latestValues } from '../../src/index.js';

export type CounterElements = Readonly<{
  output: HTMLElement;
  slowButton: HTMLButtonElement;
  fastButton: HTMLButtonElement;
  pauseButton: HTMLButtonElement;
}>;

/** Each mount creates one scope; its returned function is the unmount hook. */
export const mountCounter = (elements: CounterElements): (() => void) => {
  // null clears the display while waiting for a new timer or while paused.
  const scope = createScope<number | null>();

  // Keep source identities stable for this mount. Each connection starts at 0.
  const slow$ = interval(1000);
  const fast$ = interval(250);

  const selectedSource$ = merge(
    of(slow$),
    fromEvent(elements.slowButton, 'click').pipe(map(() => slow$)),
    fromEvent(elements.fastButton, 'click').pipe(map(() => fast$)),
    fromEvent(elements.pauseButton, 'click').pipe(map(() => null)),
  );

  // The final gate covers source selection, the active timer, and rendering.
  latestValues(selectedSource$).pipe(scope).subscribe({
    next: value => {
      elements.output.textContent = value === null ? '—' : String(value);
    },
    error: () => {
      elements.output.textContent = 'Counter failed';
    },
  });

  // The mounting runtime calls this once the view's lifetime ends.
  return scope.dispose;
};
