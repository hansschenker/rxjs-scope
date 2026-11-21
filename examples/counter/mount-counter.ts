import { fromEvent, interval, map, merge, of } from 'rxjs';
import { createScope, latestValues } from '../../src/index.js';

export type CounterElements = Readonly<{
  output: HTMLElement;
  slowButton: HTMLButtonElement;
  fastButton: HTMLButtonElement;
  pauseButton: HTMLButtonElement;
}>;

/** Mount owns one scope; its returned function is the unmount hook. */
export const mountCounter = (elements: CounterElements): (() => void) => {
  const scope = createScope();

  // Keep source identities stable for this mount. Each connection starts at 0.
  const slow$ = interval(1000);
  const fast$ = interval(250);

  const selectedSource$ = merge(
    of(slow$),
    fromEvent(elements.slowButton, 'click').pipe(map(() => slow$)),
    fromEvent(elements.fastButton, 'click').pipe(map(() => fast$)),
    fromEvent(elements.pauseButton, 'click').pipe(map(() => null)),
  );

  scope.connect(
    {
      next: value => {
        elements.output.textContent = value === null ? '—' : String(value);
      },
      error: () => {
        elements.output.textContent = 'Counter failed';
      },
    },
    latestValues(selectedSource$),
  );

  return scope.dispose;
};
