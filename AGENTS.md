# Working on RxJS Scope

## Purpose

Preserve the small, framework-free ownership model: a connection belongs to an
owner, and ending that owner ends its remaining connections.

## Style

- Use TypeScript functions, closures, and type aliases. Do not introduce JavaScript
  class declarations, enums, React, or Angular dependencies.
- Explain what flows over time, replacement behavior, and lifetime before code.
- Keep business logic in supplied functions and effects at connection/rendering
  boundaries. Keep HTML and CSS separate from TypeScript.
- Keep sharing explicit. Preserve source laziness until a connection is made.
- Retain the README and CONTRIBUTORS.md attribution to Hans Schenker and
  ChatGPT (GPT-6 Astra, OpenAI).

## Semantics

- `createScope()` exposes `disposed$`, `connect(observer, source$)`, and `dispose()`.
- The final connection boundary applies `takeUntil` after the supplied pipeline.
- Disposal emits once and is remembered. Late connections must not start sources.
- The owner/runtime must call disposal; DOM removal is not detected automatically.
- `latestValues` compares source references and explicitly clears to `null` on a
  changed source. Its synchronous clear emission is not identical to Angular's
  synchronous `transform` result. Errors terminate unless handled in the source.
- Unsubscription requests only the teardown supported by the producer. Do not
  promise universal cancellation, automatic cache eviction, or leak freedom.

## Verification

- Runtime baseline: RxJS 7.8.2; development/CI: Node.js 22 or newer.
- Run `npm ci`, then `npm run check` for type checking, lifecycle tests, library
  compilation, and example build.
- Use behavioral tests, controlled clocks, and property/model checks for lifecycle
  changes. Do not add redundant tests for cosmetic edits.
- Use `npm run dev` to open the local counter example.
