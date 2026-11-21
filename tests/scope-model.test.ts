import { expect, it } from 'vitest';
import * as fc from 'fast-check';
import { Observable, Subject } from 'rxjs';
import { createScope, latestValues } from '../src/index.js';

type SourceId = 'first' | 'second';
type Selection = SourceId | 'none';
type Action =
  | { kind: 'select'; source: Selection }
  | { kind: 'next'; source: SourceId; value: number }
  | { kind: 'dispose' };

const sourceId = fc.constantFrom<SourceId>('first', 'second');
const action: fc.Arbitrary<Action> = fc.oneof(
  {
    weight: 3,
    arbitrary: fc.record({
      kind: fc.constant('select' as const),
      source: fc.constantFrom<Selection>('first', 'second', 'none'),
    }),
  },
  {
    weight: 5,
    arbitrary: fc.record({
      kind: fc.constant('next' as const),
      source: sourceId,
      value: fc.integer(),
    }),
  },
  { weight: 1, arbitrary: fc.constant({ kind: 'dispose' as const }) },
);

it('matches an ownership model across generated selection, value, and disposal histories', () => {
  fc.assert(
    fc.property(fc.array(action, { maxLength: 80 }), actions => {
      const scope = createScope();
      const selections = new Subject<Observable<number> | null>();
      const inputs = {
        first: new Subject<number>(),
        second: new Subject<number>(),
      };
      const opened = { first: 0, second: 0 };
      const closed = { first: 0, second: 0 };
      const makeSource = (id: SourceId) =>
        new Observable<number>(observer => {
          opened[id] += 1;
          const connection = inputs[id].subscribe(observer);
          return () => {
            closed[id] += 1;
            connection.unsubscribe();
          };
        });
      const sources = {
        first: makeSource('first'),
        second: makeSource('second'),
      };
      const received: (number | null)[] = [];
      const connection = scope.connect(
        { next: value => received.push(value) },
        latestValues(selections),
      );

      // This model describes externally visible ownership, not RxJS operators.
      const expected: (number | null)[] = [];
      const expectedOpened = { first: 0, second: 0 };
      const expectedClosed = { first: 0, second: 0 };
      let previous: Selection | undefined;
      let active: SourceId | undefined;
      let disposed = false;

      const endOwner = () => {
        if (!disposed) {
          if (active !== undefined) expectedClosed[active] += 1;
          active = undefined;
          disposed = true;
        }
        scope.dispose();
      };

      for (const current of actions) {
        switch (current.kind) {
          case 'select': {
            if (!disposed && previous !== current.source) {
              if (active !== undefined) expectedClosed[active] += 1;
              previous = current.source;
              active = current.source === 'none' ? undefined : current.source;
              if (active !== undefined) expectedOpened[active] += 1;
              expected.push(null);
            }
            selections.next(current.source === 'none' ? null : sources[current.source]);
            break;
          }
          case 'next': {
            if (!disposed && active === current.source) expected.push(current.value);
            inputs[current.source].next(current.value);
            break;
          }
          case 'dispose': {
            endOwner();
            break;
          }
        }

        expect(received).toEqual(expected);
        expect(opened).toEqual(expectedOpened);
        expect(closed).toEqual(expectedClosed);
        expect(connection.closed).toBe(disposed);
      }

      endOwner();
      expect(closed).toEqual(expectedClosed);
      expect(closed).toEqual(opened);
      expect(connection.closed).toBe(true);
    }),
    { numRuns: 300 },
  );
});
