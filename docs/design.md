# Callable scopes for RxJS connections

> Every connection belongs to an owner. Ending the owner ends its remaining connections.

This design adapts the subscription ownership idea behind Angular's `AsyncPipe` to a small, class-free RxJS API. A view, dialog, animation, or task can own a scope. Stream descriptions determine what may flow; the scope determines how long its consumer may remain connected.

`createScope<T>()` returns a callable scope. Use it as `source$.pipe(scope)` or `scope(source$)`, and retain its `dispose()`, `disposed$`, and generic `connect(...)` methods. These forms all use one lifetime. “Owner” describes the application responsibility for ending that lifetime; it is not an additional public API.

The approach uses public RxJS APIs and is designed around RxJS 7.8.2. It requires a real lifecycle boundary: the application must call `dispose()` when the owner ends.

## 1. Intuition: values, source choices, and lifetime

### What flows over time

Consider a counter view with Slow, Fast, and Pause buttons. Three kinds of information participate:

- **Source choices:** Observable descriptions selecting the slow or fast counter, or `null` to select neither.
- **Counter values:** Numbers emitted by the currently selected counter.
- **Lifetime:** A notification saying that the view has ended.

The slow and fast descriptions can be created once when mounting the view. They remain on-demand descriptions until connected. Button events come from an externally live source; connecting `fromEvent` installs this view's listener. `fromEvent` adapts the element's events to an Observable, while the scope controls the lifetime of this observation. The browser's native event producer exists independently.

Clicking Fast selects the fast description. The connection to the previous counter closes, and the fast counter starts. Clicking Fast again selects the same reference, so the existing connection continues. Pause closes the counter connection. Unmounting closes the selection stream, its button listeners, and the active counter connection.

The payload could equally be a search result, account balance, or animation position. Ownership does not inspect that payload.

### What Angular contributes

The uploaded walk-through of Angular's async pipe identifies a useful arrangement: the pipe remembers its selected source, subscribes to it, replaces that connection when the reference changes, and disposes it when its view is destroyed. Angular also stores the latest value and integrates it with change detection.[^angular]

The transferable idea is **an explicit owner for the connection**. In a plain RxJS view runtime, mounting creates the owner, and unmounting ends it. Rendering remains an output effect supplied by the application.

This also defines the limit of “automatic.” Removing an element alone does not tell RxJS that an owner has ended. The router, mounting runtime, or task coordinator must invoke the registered teardown. After that hook is connected, each stream attached through the scope inherits the same lifetime.

### Two independent policies

**Replacement** answers which source is relevant now. `distinctUntilChanged()` compares consecutive source references using `===`; `switchMap` closes the previous inner connection before connecting to its replacement.[^distinct][^switch]

**Lifetime** answers how long the observation may exist. A scope supplies a disposal notification, and `takeUntil` closes each scoped connection when that notification arrives. The stop notification stays separate from the stream's business values.[^until]

For a fixed source, only ownership is needed. Source switching is useful when a view can select another Observable during its lifetime.

## 2. Specification: the small public contract

### `createScope<T>()`

The factory returns a function with lifecycle members:

| Use or member | Meaning |
| --- | --- |
| `source$.pipe(scope)` | Compose a stream with the scope's lifetime boundary. |
| `scope(source$)` | Apply the same operator directly; the result remains lazy. |
| `disposed$: Observable<void>` | A read-only view of the scope's retained disposal signal. |
| `connect(observer, source$): Subscription` | Connect the supplied observer to the source under this scope's lifetime. |
| `dispose(): void` | End the scope and close its remaining connections. Repeated calls have no additional effect. |

The public callable type is:

```ts
type ScopedOperator<T> = Scope & MonoTypeOperatorFunction<T>;

declare const createScope: <T = unknown>() => ScopedOperator<T>;
```

A function can carry properties. Each public factory call creates one internal `createScopeCore()`, creates an operator applying `takeUntil(core.disposed$)`, and combines them with `Object.assign`. The core retains the original lifecycle mechanism. Applying or subscribing through the callable scope does not create another core or lifetime.

Creating the scope and composing it with a source do not connect any business source. Calling `subscribe` on the resulting Observable, or calling `scope.connect`, is the explicit subscription boundary. The method's observer-first argument order remains available for existing code.

`T` describes the values accepted and preserved by the callable operator. The lifecycle signal itself contains no business value. `connect` remains independently generic, so a `createScope<number>()` may also attach a string-valued stream through its method. Plain `createScope()` still works for method-style use; its callable type defaults to `unknown` when no type information is supplied. Specify `<T>` for an independently declared callable scope.

Retain the returned scope handle so the runtime can register `scope.dispose` as teardown. Inline creation inside `pipe` loses that useful handle. One scope can be applied to several pipelines; they share a lifetime, while each source retains its existing on-demand, live, or explicitly shared execution behavior.

Each connection can complete, error, or be closed early through its returned `Subscription`. That closes the individual connection without disposing the entire scope. A disposed scope is never reopened; a new mount creates a new scope.

The scope manages connections made through it and subscriptions owned by those composed pipelines. An unrelated subscription created separately has its own lifetime.

### `latestValues(sources$)`

This function transforms:

```ts
Observable<Observable<T> | null | undefined>
```

into:

```ts
Observable<T | null>
```

It uses source reference equality, follows the current source, and emits `null` to clear the old presentation when the source changes or becomes absent. It is a lazy stream description; subscription begins at a downstream connection.

The reset value is a rendering policy. If `T` itself includes meaningful `null` values, a view may need explicit states such as `waiting`, `value`, and `error` to distinguish them. The minimal helper intentionally has the simpler nullable result.

### Observable behavior

| Event | Required behavior |
| --- | --- |
| Construct a scope or `latestValues` pipeline, or apply a scope to a source | No business source starts. |
| Connect through an active scope | Subscribe under the owner's lifetime. |
| One scoped connection completes or is closed early | Close that connection; the scope and its siblings remain available. |
| Select a source | Clear the previous presentation and connect to that source. |
| Select the same reference again | Keep the current connection; do not restart or clear it. |
| Select a different reference | Close the old source before connecting to the replacement. |
| Select `null` or `undefined` | Close the current source and emit `null`; keep listening for later selections. |
| The current inner source completes | Close that inner connection; later source choices are still allowed. |
| The source-selection stream completes | Accept no more selections; the current inner may continue until it ends or is cancelled. |
| An unhandled source error occurs | Error and close that binding. Other scope connections remain independent. |
| Dispose the scope | Complete owned outputs and close their upstream connections. |
| Connect after disposal | Complete without starting the supplied source. |

### Why disposal is retained

A plain `Subject` can notify existing connections, but a connection created later misses the earlier event. A `ReplaySubject<void>(1)` retains one disposal notification, including after completion.[^replay]

In RxJS 7.8.2, `takeUntil` subscribes to its notifier first and checks whether its downstream subscriber is already closed before subscribing to the source. The retained notification therefore prevents a late source from starting.[^until]

Disposal must emit before completing the notifier. Completing a notifier without a value does not trigger `takeUntil`. Likewise, a `BehaviorSubject(false)` is unsuitable as a direct stop notifier: its initial `false` is still a notification and would immediately stop a connection.

### Place the callable scope at the lifetime boundary

`scope.connect` applies `takeUntil(disposed$)` after the supplied pipeline. With the callable form, the caller chooses that position: put `scope` downstream of flattening, scheduling, resubscription, and sharing whose consumer lifetime it should control.

```ts
const scope = createScope<number | null>();

latestValues(selectedSource$).pipe(scope).subscribe(renderValue);
```

Putting the lifetime boundary before `switchMap` only completes its stream of source choices. `switchMap` waits for its active inner to finish before completing, so an ongoing inner timer may remain connected. Applying the lifetime boundary at the output closes both the selection connection and the active inner.[^switch]

The same placement principle applies to `mergeMap` and `concatMap`, which can retain active or queued inner work, and to scheduling operators such as `delay`, which can retain pending deliveries. A final scope boundary cancels the work owned upstream of that boundary.[^merge][^delay]

Placing `repeat` after the scope lets disposal completion trigger another subscription. Because disposal is remembered, that subscription can complete immediately and cause further repeats. Place `repeat` before the scope so disposal closes the entire resubscription process.[^repeat]

For per-consumer lifetime, apply the scope after explicit sharing. Otherwise, a completed replay cache downstream of the scope may still deliver values to a later subscriber. The sharing policy continues to decide whether other consumers or a non-reference-counted connection keep upstream running.[^share]

This is a placement rule for operations that can extend work or delivery. `finalize` may follow the scope to observe connection cleanup.

This also protects synchronous setup. If the consumer disposes its scope while handling a synchronous value, subsequent values are not delivered through that closed connection. Cancellation does not forcibly interrupt arbitrary JavaScript computation: a custom producer must cooperate with its closed subscriber and provide teardown for resources it creates.

## 3. Rendering, errors, and resources

### Preserve synchronous behavior deliberately

The original article's statement that the first async-pipe value is always `null` is too strong. Angular subscribes before returning its stored value. A synchronous source such as `of(42)` can therefore supply the returned value immediately.[^angular-source]

`latestValues` makes a separate choice: `startWith(null)` emits a clearing notification before every newly selected non-null source. A synchronous source can consequently deliver `null` and its value in the same call stack. This reproduces the ownership policy while exposing intermediate rendering state explicitly.

### Completion and cancellation carry different meanings

Directly unsubscribing does not call the observer's `complete` handler. A disposal notification reaching `takeUntil` does complete its downstream observer while tearing down upstream.[^until][^glossary]

For an owned output, completion means that observation ended. It is not sufficient evidence that the original operation finished its intended work. Use an explicit result value for business success.

Cleanup needed for completion, errors, and unsubscription belongs in source teardown or `finalize`. `finalize` observes the end; it does not supply the lifetime decision.[^finalize]

### Choose whether a binding survives source errors

The minimal implementation follows normal RxJS error propagation. If an inner source errors, the switching pipeline terminates and stops observing future source choices.

To keep accepting selections after a failure, handle the error inside the `switchMap` branch and represent it as a view-state value. Catching outside the switching operation replaces the entire selection pipeline instead. Error handling should preserve a visible failure state or reporting path rather than silently hide failures.

### Sharing is a separate choice

Neither `createScope` nor `latestValues` implicitly shares a source. Reusing a callable scope shares only its lifetime. Two subscriptions to an on-demand source can create two executions. If consumers should share one execution, supply an explicitly shared pipeline:

```ts
const scope = createScope<number | null>();

const sharedValue$ = latestValues(selectedSource$).pipe(
  shareReplay({ bufferSize: 1, refCount: true }),
);

sharedValue$.pipe(scope).subscribe(firstObserver);
sharedValue$.pipe(scope).subscribe(secondObserver);
```

Reference counting lets an ongoing upstream connection close when its last consumer leaves. Another owner's remaining connection can keep the shared producer running. The shorthand `shareReplay(1)` defaults to `refCount: false` and can keep an upstream source connected with no consumers.[^share]

Completed replay caches have a separate lifetime: `shareReplay` retains successfully completed values even with reference counting enabled. The scope's final lifetime gate prevents a late connection through a disposed scope from receiving cached values. It does not erase caches or other references held elsewhere. A different cache-reset policy must be chosen explicitly when required.[^share]

### Teardown depends on the producer

Unsubscription releases resources through the teardown supplied by the source. Custom sources must return cleanup for resources they create. A connection to an externally shared producer may end while the producer continues for other consumers.

Wrapping an already-created Promise does not make its work cancellable. For cancellable fetch work, `fromFetch` ties its subscription to an `AbortController`; its selector option can keep response-body consumption inside that cancellation boundary.[^fetch]

Ownership, producer cleanup, and cache retention are distinct responsibilities. A scope gives a consistent lifetime rule without claiming universal freedom from memory leaks.

## 4. Verification: observable obligations

Tests should observe connection counts, teardown order, delivered values, and termination. They should establish the contract without depending on private operator state.

Useful Given–When–Then obligations include:

1. **Laziness.** Given a source that records connections, when a scope and binding are constructed or the scope is applied to a source, then the connection count remains zero.
2. **One lifetime across forms.** Given connections made through `.pipe(scope)`, direct `scope(source$)`, and `scope.connect(...)`, when that scope is disposed, then all remaining connections close.
3. **Stable identity.** Given an active source, when the same reference is selected again, then its connection is reused.
4. **Replacement order.** Given source A, when B is selected, then A's teardown occurs before B connects.
5. **Whole-scope cleanup.** Given several scoped observations, when the scope is disposed, then their selection and inner connections close.
6. **Late setup.** Given an already-disposed scope, when a connection is requested through any supported form, then its source never starts.
7. **Synchronous disposal.** Given a synchronous source, when handling its first value disposes the scope, then no later value reaches that consumer and registered teardown runs once.
8. **Independent completion.** Given one completed connection, then sibling connections remain available. Given a completed selected inner source, when a different source is selected, then the new source can connect.
9. **Declared error policy.** Given a source failure, then the minimal binding terminates, or an explicitly recovering composition exposes the failure and accepts later selections.
10. **Explicit sharing.** Given two consumers of a shared ongoing source, when one leaves, then the other continues; when the last leaves, then upstream teardown occurs.
11. **Retained disposal.** Given a completed replay cache and a disposed scope, when another connection is requested through that scope, then no cached value is delivered.

Use deterministic sources or a virtual scheduler for temporal assertions. Source completion, manual early closure, and owner disposal should be checked as separate events because their notification meanings differ.

## 5. Placement in a view runtime

The library supplies the reusable callable scope. The application runtime creates a scope for each mounted view, connects its streams, and registers `dispose` as teardown. Source replacement stays in the dataflow. Rendering stays at the connection boundary. Application-wide state can have an application lifetime while each view owns only its observation of that state.

The [counter's mounting function](../examples/counter/mount-counter.ts) creates `createScope<number | null>()`, subscribes to `latestValues(selectedSource$).pipe(scope)`, and returns `scope.dispose`. The [page controller](../examples/counter/main.ts) invokes that cleanup before removing the view and creates a fresh scope on remount. The page's own connections also demonstrate compatible `createScope()` plus method-style `connect` use.

The resulting teaching rule is:

- **While** the owner is active, its connections may receive values.
- **When** a replacement source arrives, the switching policy closes the previous connection.
- **When** the owner ends, its remaining connections close.
- **When** setup arrives after disposal, no new source execution begins.

## Primary references

[^angular]: [Angular: AsyncPipe API and lifetime contract](https://angular.dev/api/common/AsyncPipe).
[^angular-source]: [Angular: AsyncPipe implementation](https://github.com/angular/angular/blob/main/packages/common/src/pipes/async_pipe.ts). The `main` implementation evolves; the discussion uses the source inspected on 21 November 2026.
[^distinct]: [RxJS 7.8.2: distinctUntilChanged implementation](https://github.com/ReactiveX/rxjs/blob/7.8.2/src/internal/operators/distinctUntilChanged.ts).
[^switch]: [RxJS 7.8.2: switchMap implementation](https://github.com/ReactiveX/rxjs/blob/7.8.2/src/internal/operators/switchMap.ts).
[^merge]: [RxJS 7.8.2: shared merging and queueing implementation](https://github.com/ReactiveX/rxjs/blob/7.8.2/src/internal/operators/mergeInternals.ts).
[^delay]: [RxJS 7.8.2: delay implementation](https://github.com/ReactiveX/rxjs/blob/7.8.2/src/internal/operators/delay.ts).
[^repeat]: [RxJS 7.8.2: repeat implementation](https://github.com/ReactiveX/rxjs/blob/7.8.2/src/internal/operators/repeat.ts).
[^until]: [RxJS 7.8.2: takeUntil implementation](https://github.com/ReactiveX/rxjs/blob/7.8.2/src/internal/operators/takeUntil.ts).
[^replay]: [RxJS 7.8.2: ReplaySubject implementation](https://github.com/ReactiveX/rxjs/blob/7.8.2/src/internal/ReplaySubject.ts).
[^glossary]: [RxJS: Glossary and semantics](https://rxjs.dev/guide/glossary-and-semantics).
[^finalize]: [RxJS: finalize](https://rxjs.dev/api/operators/finalize).
[^share]: [RxJS 7.8.2: shareReplay implementation and reset policies](https://github.com/ReactiveX/rxjs/blob/7.8.2/src/internal/operators/shareReplay.ts).
[^fetch]: [RxJS: fromFetch and cancellation](https://rxjs.dev/api/fetch/fromFetch).
