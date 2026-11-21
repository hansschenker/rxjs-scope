import { filter, fromEvent, take } from 'rxjs';
import { createScope } from '../../src/index.js';
import { mountCounter } from './mount-counter.js';
import './style.css';

const requiredElement = <T extends Element>(selector: string): T => {
  const element = document.querySelector<T>(selector);
  if (element === null) throw new Error(`Missing element: ${selector}`);
  return element;
};

const host = requiredElement<HTMLElement>('#counter-view');
const status = requiredElement<HTMLElement>('#scope-status');
const mountButton = requiredElement<HTMLButtonElement>('#mount');
const unmountButton = requiredElement<HTMLButtonElement>('#unmount');
const template = requiredElement<HTMLTemplateElement>('#counter-template');

// The page controls use the compatible connect form and a separate lifetime.
// Every mounted counter demonstrates createScope<T>() with .pipe(scope).
const pageScope = createScope();
let unmountCurrent: (() => void) | undefined;

const unmountView = (): void => {
  unmountCurrent?.();
  unmountCurrent = undefined;
  host.replaceChildren();
  status.textContent = 'Unmounted · counter and view listeners closed';
  mountButton.disabled = false;
  unmountButton.disabled = true;
};

const mountView = (): void => {
  if (unmountCurrent !== undefined) return;

  host.replaceChildren(template.content.cloneNode(true));
  unmountCurrent = mountCounter({
    output: requiredElement<HTMLElement>('#value'),
    slowButton: requiredElement<HTMLButtonElement>('#slow'),
    fastButton: requiredElement<HTMLButtonElement>('#fast'),
    pauseButton: requiredElement<HTMLButtonElement>('#pause'),
  });

  status.textContent = 'Mounted · one view scope';
  mountButton.disabled = true;
  unmountButton.disabled = false;
};

const reportError = (error: unknown): void => {
  status.textContent = `Unable to update the view: ${String(error)}`;
};

pageScope.connect(
  { next: mountView, error: reportError },
  fromEvent(mountButton, 'click'),
);

pageScope.connect(
  { next: unmountView, error: reportError },
  fromEvent(unmountButton, 'click'),
);

pageScope.connect(
  {
    next: () => {
      unmountView();
      pageScope.dispose();
    },
    error: reportError,
  },
  // A page kept in the browser's back/forward cache still owns its scope.
  fromEvent<PageTransitionEvent>(window, 'pagehide').pipe(
    filter(event => !event.persisted),
    take(1),
  ),
);

mountView();
