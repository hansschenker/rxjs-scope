import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { mountCounter } from '../examples/counter/mount-counter.js';

const createButton = () => {
  const target = new EventTarget();
  const added = vi.spyOn(target, 'addEventListener');
  const removed = vi.spyOn(target, 'removeEventListener');

  return {
    element: target as unknown as HTMLButtonElement,
    click: () => target.dispatchEvent(new Event('click')),
    added,
    removed,
  };
};

const createCounterFixture = () => {
  const output = { textContent: '' };
  const slow = createButton();
  const fast = createButton();
  const pause = createButton();

  return {
    output,
    slow,
    fast,
    pause,
    elements: {
      output: output as unknown as HTMLElement,
      slowButton: slow.element,
      fastButton: fast.element,
      pauseButton: pause.element,
    },
  };
};

beforeEach(() => {
  vi.useFakeTimers();
});

afterEach(() => {
  vi.clearAllTimers();
  vi.useRealTimers();
  vi.restoreAllMocks();
});

describe('counter mounting with a callable scope', () => {
  it('preserves slow/fast identity, clears changed selections, and pauses or resumes the timer', () => {
    const view = createCounterFixture();
    const dispose = mountCounter(view.elements);

    expect(view.output.textContent).toBe('—');
    expect(vi.getTimerCount()).toBe(1);
    vi.advanceTimersByTime(999);
    expect(view.output.textContent).toBe('—');
    vi.advanceTimersByTime(1);
    expect(view.output.textContent).toBe('0');

    // Re-selecting the same source keeps its current value and next due time.
    vi.advanceTimersByTime(500);
    view.slow.click();
    expect(view.output.textContent).toBe('0');
    vi.advanceTimersByTime(500);
    expect(view.output.textContent).toBe('1');

    view.fast.click();
    expect(view.output.textContent).toBe('—');
    expect(vi.getTimerCount()).toBe(1);
    vi.advanceTimersByTime(249);
    expect(view.output.textContent).toBe('—');
    vi.advanceTimersByTime(1);
    expect(view.output.textContent).toBe('0');
    vi.advanceTimersByTime(250);
    expect(view.output.textContent).toBe('1');
    view.fast.click();
    expect(view.output.textContent).toBe('1');
    vi.advanceTimersByTime(250);
    expect(view.output.textContent).toBe('2');

    view.pause.click();
    expect(view.output.textContent).toBe('—');
    expect(vi.getTimerCount()).toBe(0);
    vi.advanceTimersByTime(5000);
    expect(view.output.textContent).toBe('—');

    view.slow.click();
    expect(view.output.textContent).toBe('—');
    vi.advanceTimersByTime(1000);
    expect(view.output.textContent).toBe('0');
    dispose();
    expect(vi.getTimerCount()).toBe(0);
  });

  it('removes all three event listeners and stops scheduled work during unmount', () => {
    const view = createCounterFixture();
    const buttons = [view.slow, view.fast, view.pause];
    const dispose = mountCounter(view.elements);
    vi.advanceTimersByTime(1000);

    for (const button of buttons) {
      expect(button.added).toHaveBeenCalledTimes(1);
      expect(button.removed).not.toHaveBeenCalled();
    }

    dispose();
    dispose();
    expect(vi.getTimerCount()).toBe(0);

    for (const button of buttons) {
      expect(button.removed).toHaveBeenCalledTimes(1);
      expect(button.removed.mock.calls[0]?.[0]).toBe('click');
      expect(button.removed.mock.calls[0]?.[1]).toBe(button.added.mock.calls[0]?.[1]);
      button.click();
    }

    vi.advanceTimersByTime(5000);
    expect(view.output.textContent).toBe('0');
    expect(vi.getTimerCount()).toBe(0);
  });

  it('creates a fresh lifetime on remount that an old cleanup cannot end', () => {
    const view = createCounterFixture();
    const firstDispose = mountCounter(view.elements);
    vi.advanceTimersByTime(1000);
    expect(view.output.textContent).toBe('0');
    firstDispose();

    const secondDispose = mountCounter(view.elements);
    expect(view.output.textContent).toBe('—');
    firstDispose();
    expect(vi.getTimerCount()).toBe(1);

    view.fast.click();
    vi.advanceTimersByTime(250);
    expect(view.output.textContent).toBe('0');
    vi.advanceTimersByTime(250);
    expect(view.output.textContent).toBe('1');

    secondDispose();
    expect(vi.getTimerCount()).toBe(0);
    for (const button of [view.slow, view.fast, view.pause]) {
      expect(button.added).toHaveBeenCalledTimes(2);
      expect(button.removed).toHaveBeenCalledTimes(2);
    }
  });
});
