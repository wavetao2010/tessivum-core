import { expect, test } from 'bun:test'
import { execute } from './scenarios.ts'
import type { Fixture } from './types.ts'

test('event catalog executes and compares each case instead of ignoring the case list', async () => {
  const fixture = await Bun.file(new URL('../../fixtures/conformance/events.json', import.meta.url)).json() as Fixture
  const cases = fixture.input!.cases as { id: string }[]
  for (const [index, item] of cases.entries()) {
    const single: Fixture = { ...fixture, input: { cases: [item] }, expectedTrace: [fixture.expectedTrace[index]!] }
    const actual = await execute(single)
    expect(actual.status).toBe('PASS')
    expect(actual.trace).toEqual(single.expectedTrace)
    const changed: Fixture = { ...single, expectedTrace: [{ ...single.expectedTrace[0]!, value: null }] }
    expect((await execute(changed)).status).toBe('MISMATCH')
  }
  for (const cases of [[{ id: 'unimplemented-event-case' }], [], [null], [{ id: '' }]]) {
    expect((await execute({ ...fixture, input: { cases } })).status).toBe('ORACLE_ERROR')
  }
})

test('lifecycle catalog executes each declared case and rejects malformed cases', async () => {
  const fixture = await Bun.file(new URL('../../fixtures/conformance/lifecycle.json', import.meta.url)).json() as Fixture
  for (const [index, item] of (fixture.input!.cases as { id: string }[]).entries()) {
    const single: Fixture = { ...fixture, input: { cases: [item] }, expectedTrace: [fixture.expectedTrace[index]!] }
    const actual = await execute(single)
    expect(actual.status).toBe('PASS')
    expect(actual.trace).toEqual(single.expectedTrace)
    const changed: Fixture = { ...single, expectedTrace: [{ ...single.expectedTrace[0]!, value: null }] }
    expect((await execute(changed)).status).toBe('MISMATCH')
  }
  for (const cases of [[{ id: 'unknown' }], [], [null], [{ id: '' }]]) {
    expect((await execute({ ...fixture, input: { cases } })).status).toBe('ORACLE_ERROR')
  }
})

test('service catalog executes each declared case and rejects malformed cases', async () => {
  const fixture = await Bun.file(new URL('../../fixtures/conformance/services.json', import.meta.url)).json() as Fixture
  for (const [index, item] of (fixture.input!.cases as { id: string }[]).entries()) {
    const single: Fixture = { ...fixture, input: { cases: [item] }, expectedTrace: [fixture.expectedTrace[index]!] }
    const actual = await execute(single)
    expect(actual.status).toBe('PASS')
    expect(actual.trace).toEqual(single.expectedTrace)
    const changed: Fixture = { ...single, expectedTrace: [{ ...single.expectedTrace[0]!, value: null }] }
    expect((await execute(changed)).status).toBe('MISMATCH')
  }
  for (const cases of [[{ id: 'unknown' }], [], [null], [{ id: '' }]]) {
    expect((await execute({ ...fixture, input: { cases } })).status).toBe('ORACLE_ERROR')
  }
})

test('service registration executes the three strict cases', async () => {
  const fixture = await Bun.file(new URL('../../fixtures/conformance/service-registration.json', import.meta.url)).json() as Fixture
  const actual = await execute(fixture)
  expect(actual.status).toBe('PASS')
  expect(actual.trace).toEqual(fixture.expectedTrace)

  const changed: Fixture = { ...fixture, expectedTrace: fixture.expectedTrace.map((event) => ({ ...event, value: null })) }
  expect((await execute(changed)).status).toBe('MISMATCH')

  for (const cases of [[{ id: 'unknown' }], [], [null], [{ id: '' }], [{ id: 'same-owner-duplicate', extra: true }]]) {
    expect((await execute({ ...fixture, input: { cases } })).status).toBe('ORACLE_ERROR')
  }
})

test('loader catalog executes each case and never accepts an empty or changed baseline', async () => {
  const fixture = await Bun.file(new URL('../../fixtures/conformance/loader.json', import.meta.url)).json() as Fixture
  for (const [index, item] of (fixture.input!.cases as { id: string }[]).entries()) {
    const single: Fixture = { ...fixture, input: { cases: [item] }, expectedTrace: [fixture.expectedTrace[index]!] }
    const actual = await execute(single)
    expect(actual.status).toBe('PASS')
    expect(actual.trace).toEqual(single.expectedTrace)
    expect((await execute({ ...single, expectedTrace: [{ ...single.expectedTrace[0]!, value: null }] })).status).toBe('MISMATCH')
  }
  for (const cases of [[{ id: 'unknown' }], [], [null], [{ id: '' }]]) {
    expect((await execute({ ...fixture, input: { cases } })).status).toBe('ORACLE_ERROR')
  }
  expect((await execute({ ...fixture, expectedTrace: [] })).status).toBe('MISMATCH')
})
