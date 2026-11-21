# RxJS Scope

**Explicit ownership and automatic connection teardown with plain RxJS.**

> A connection may remain active only while its owner remains active.

Created with Hans Schenker. **ChatGPT (GPT-6 Astra, OpenAI) is the main contributor
and co-author of the initial design, implementation, examples, tests, and
documentation.** See [Contributors](CONTRIBUTORS.md).

## The idea

A view, route, animation, or task has a lifetime. Its connections belong to that
lifetime. When its owner ends, those connections end together.

Angular's async pipe inspired this project: it owns a connection, replaces it
when the selected source reference changes, and closes it when its view is
destroyed. RxJS Scope expresses those ownership rules using ordinary RxJS
operators and a small function-based API.

The application supplies the real lifecycle boundary. A mounting runtime creates
a scope and invokes its disposal function on unmount. Removing a DOM element by
itself does not dispose a scope.

| Responsibility | Mechanism |
| --- | --- |
| Describe which values should flow | An Observable pipeline |
| Follow a newly selected source | `latestValues`: reference comparison and `switchMap` |
| Bound the lifetime of an observation | `createScope`: a final `takeUntil` boundary |
| Interpret values for the application | User-supplied functions and render sinks |
| Share an execution between consumers | An explicitly configured sharing operator |

The domain can change. The ownership machinery stays the same.

## Run the project

Use Node.js 22 or newer. The runtime baseline is **RxJS 7.8.2**.

```sh
git clone https://github.com/hansschenker/rxjs-scope.git
cd rxjs-scope
npm ci
npm run check
npm run dev
```

Open the local URL printed by Vite for the counter example. `npm run check` runs
strict TypeScript checking, lifecycle tests, the library build, and the example
build. The package is private; these instructions use the repository directly.

## 1. Give a connection an owner

`createScope()` creates local lifecycle bookkeeping. It does not subscribe to
your source. `connect(observer, source$)` starts the observation and attaches it
to that lifetime.

```ts
import { interval } from 'rxjs';
import { createScope } from './src/index.js';

const scope = createScope();
const count$ = interval(1000);

scope.connect(
  {
    next: value => console.log(value),
    error: error => console.error(error),
  },
  count$,
);

// Register this function with the owner's actual unmount/cancel mechanism.
const unmount = scope.dispose;
```

The owner invokes `unmount()` when it ends. A scope may own many connections;
disposing it ends all the remaining ones. A returned `Subscription` also allows
one connection to be closed early.

### Scope contract

```ts
type Scope = Readonly<{
  disposed$: Observable<void>;
  connect: <T>(
    observer: Partial<Observer<T>>,
    source$: Observable<T>,
  ) => Subscription;
  dispose: () => void;
}>;
```

- **While** the scope is active, its connections may receive values.
- **When** the scope is disposed, its remaining connections close.
- **When** disposal is repeated, nothing further happens.
- **When** a connection is requested after disposal, the source does not start.

Disposal is remembered with `ReplaySubject<void>(1)`. The final connection
boundary applies `takeUntil(disposed$)` after the supplied pipeline, so it also
closes active inner subscriptions created by flattening operators.

## 2. Follow the selected source

`latestValues` receives a stream whose values are source descriptions. It returns
the values from the currently selected source.

```ts
import { BehaviorSubject, interval } from 'rxjs';
import type { Observable } from 'rxjs';
import { createScope, latestValues } from './src/index.js';

const slow$ = interval(1000);
const fast$ = interval(250);
const selectedSource$ = new BehaviorSubject<Observable<number> | null>(slow$);
const scope = createScope();

scope.connect(
  { next: value => console.log(value) },
  latestValues(selectedSource$),
);

// These illustrate source-selection actions over the owner's lifetime.
selectedSource$.next(fast$); // Close slow; clear the view; connect fast.
selectedSource$.next(fast$); // Same reference: keep the existing connection.
selectedSource$.next(null);  // Close fast and emit the clear-view marker.
scope.dispose();            // Close the selection connection as well.
```

The scope observes the selection subject; it does not own or globally complete
that subject. Source descriptions remain lazy. The caller decides whether a
producer is on demand, externally live, or explicitly shared.

| Selection or notification | Result |
| --- | --- |
| A different Observable reference | Close the previous source before connecting to the replacement |
| The same consecutive reference | Keep the current connection, including when that source already completed |
| `null` or `undefined` | Disconnect the current source and emit `null` |
| A selected source emits | Forward its value |
| A selected source completes | Keep listening for future source selections |
| The selection stream completes | Let the current source finish; scope disposal still cancels it |
| An unhandled source error | Error and close the binding |
| The scope is disposed | Close both selection and current-source connections |

`startWith(null)` is an explicit clear-view policy on a changed source. It emits
`null` even before a synchronous source emits. Angular's synchronous `transform`
can return the value immediately, so this rendering detail is deliberately
different. Model loading/error/value states separately when `null` is meaningful
application data.

## 3. Put ownership in mounting

The [counter example](examples/counter/mount-counter.ts) returns its scope's
disposal function to its mounting system. Its **Slow**, **Fast**, and **Pause**
buttons select stable source references; the timers supply the timing.

The [page controller](examples/counter/main.ts) calls that function before
removing the view. Every remount receives a fresh scope.

- A changed speed closes the previous timer and starts the selected one at zero.
- Repeatedly selecting the same speed keeps its connection running.
- Pause closes the timer while leaving the view mounted.
- Unmount closes the timer and the view's button listeners together.

HTML, CSS, and TypeScript are separate. There are no framework dependencies,
JavaScript class declarations, enums, or `tap` effects in the example pipelines.

## Behavior to keep explicit

**Cancellation and completion.** Disposal completes the managed output and
unsubscribes upstream. This differs from directly calling `unsubscribe()`, which
does not send an observer a `complete` notification. Completion of the managed
output therefore means the observation ended, not necessarily that the original
operation succeeded.

**Error policy.** An unhandled error ends its binding without disposing sibling
connections. To continue accepting source replacements after an inner failure,
handle the error inside the `switchMap` branch and represent it as a view state.

**Sharing.** Nothing is shared implicitly. For a shared ongoing source, choose
`shareReplay({ bufferSize: 1, refCount: true })` when last-consumer teardown is
wanted. `shareReplay(1)` defaults to `refCount: false`; even reference-counted
sharing can retain completed caches. Scope disposal is not cache eviction.

**Producer cleanup.** Unsubscription invokes the teardown supported by the
producer. It cannot stop an already-running Promise, other owners' connections,
or custom work that has no teardown. For cancellable fetch work, consider
RxJS's `fromFetch` and its request/body cancellation contract.

**Separate subscriptions.** Only connections registered with the scope, or
belonging to their composed subscription tree, inherit its lifetime. An unrelated
manual subscription needs its own owner.

## Intuition → Specification → Verification

The [design document](docs/design.md) develops the ownership model, Angular
comparison, contracts, edge cases, and Given–When–Then obligations.

Tests exercise observable behavior: connection counts, replacement ordering,
teardown, completion, synchronous disposal, late connection prevention, error
isolation, sharing, and type inference. A fast-check model explores generated
histories of source selection, values, and owner disposal. Deterministic tests
also cover closing individual connections early.

```sh
npm run typecheck
npm test
npm run build
npm run build:example
```

The GitHub Actions workflow runs the same combined `npm run check` gate.

## Contributors

| Contributor | Contribution |
| --- | --- |
| [Hans Schenker](https://github.com/hansschenker) | Project initiator, maintainer, teaching direction, and collaborative design |
| **ChatGPT (GPT-6 Astra, OpenAI)** | **Main contributor and co-author:** initial scope design, implementation, examples, lifecycle tests, and documentation |

See the maintained [contributor list](CONTRIBUTORS.md). The implementation was
developed from the scope discussion with Hans Schenker on 21 November 2026.

## Primary references

- [Angular AsyncPipe API](https://angular.dev/api/common/AsyncPipe)
- [Angular AsyncPipe implementation](https://github.com/angular/angular/blob/main/packages/common/src/pipes/async_pipe.ts)
- [RxJS 7.8.2 takeUntil implementation](https://github.com/ReactiveX/rxjs/blob/7.8.2/src/internal/operators/takeUntil.ts)
- [RxJS 7.8.2 switchMap implementation](https://github.com/ReactiveX/rxjs/blob/7.8.2/src/internal/operators/switchMap.ts)
- [RxJS 7.8.2 shareReplay implementation](https://github.com/ReactiveX/rxjs/blob/7.8.2/src/internal/operators/shareReplay.ts)
- [RxJS fromFetch](https://rxjs.dev/api/fetch/fromFetch)
