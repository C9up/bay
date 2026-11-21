# @c9up/bay

> Pluggable job-queue contract for the Ream framework, with memory + Redis drivers.

Part of **[Ream](https://github.com/C9up/ream)** — a Rust-powered Node.js framework. Independent, publishable package.

## Installation

```bash
pnpm add @c9up/bay
ream configure @c9up/bay
```

`ream add @c9up/bay` installs it, registers the provider and writes
`config/queue.ts`. The rest of this page assumes that has run.

## Usage

Register the provider, then name the queue backend in `config/queue.ts` — the
provider reads `config.get('queue')`, so a `config/bay.ts` would be loaded under
the key `bay` and never seen:

```ts
// reamrc.ts
providers: [
  () => import('@c9up/bay/provider'),
]
```

```ts
// config/queue.ts
import { defineConfig, drivers } from '@c9up/bay'
import env from '#start/env'

export default defineConfig({
  default: env.get('QUEUE_DRIVER'),
  adapters: {
    memory: drivers.memory(),
    redis:  drivers.redis({ connection: 'main' }),
  },
})
```

The keys are the framework's: `default` + `adapters`, filled from a `drivers`
namespace, selected by `QUEUE_DRIVER`. Bay used to say `stores` / `QUEUE_STORE`;
both names still resolve, so an existing `config/queue.ts` keeps working.

## A job is a class

```ts
// app/jobs/send_email.ts  —  ream make:job SendEmail
import { Job, exponentialBackoff } from '@c9up/bay'
import type { JobOptions } from '@c9up/bay'

export default class SendEmail extends Job<{ to: string }> {
  static options: JobOptions = {
    queue: 'emails',   // which queue it waits in
    maxRetries: 5,     // how many times the handler may run
    delay: '10s',      // hold it before any worker may take it
    timeout: '1m',     // how long the handler gets
    retry: { backoff: exponentialBackoff() },  // 1s, 2s, 4s… between attempts
  }

  async execute() {
    await mail.send(this.payload.to)
  }

  async failed(error: Error) {
    // Once the last attempt has failed — the alert, not the retry.
  }
}
```

```ts
import queue from '@c9up/bay/services/main'

await queue.dispatch(SendEmail, { to: 'user@example.com' })
```

The class carries its own name, its options and the type of the payload it
reads, so a field the handler reads cannot be one the dispatcher never sent. The
call site overrides any of them: `dispatch(SendEmail, payload, { queue: 'critical' })`.

A job is built by the container, so it takes its dependencies in its
constructor the way a controller does:

```ts
@inject()
export default class SendInvoice extends Job<{ id: number }> {
  constructor(private mailer: Mailer, private invoices: InvoiceRepository) {
    super()
  }
}
```

`jobFactory` in `config/queue.ts` replaces the container when a job needs
building some other way; a factory that throws is a failed attempt, retried like
any other.

Registering by name still works, and is what a job whose name is computed at
runtime needs:

```ts
queue.register('send-email', new SendEmailHandler())
await queue.dispatch('send-email', { to: 'user@example.com' })
```

### What a running job knows

`this.context` is the attempt: `jobId`, `name`, `attempt` (1 on the first run),
`queue`, `priority`, `acquiredAt` and `stalledCount`, frozen. A handler
registered by name receives it as the third argument of `handle(payload,
signal, context)`.

```ts
async execute() {
  if (this.context.attempt > 1) logger.info(`retrying ${this.context.jobId}`)
}
```

### Dispatching from the class

```ts
const { jobId } = await SendEmail.dispatch({ to }).toQueue('emails').in('5m').priority(1)
const { jobIds } = await SendEmail.dispatchMany([{ to: a }, { to: b }]).toQueue('emails')
```

Both go through the queue bound to `services/main`; awaiting the builder sends
it. `dedup({ id, ttl?, extend? })` is on the single dispatcher. Upstream's
`group()` and `with(adapter)` are not offered: bay has no job groups, and one
queue drives one store. `dispatchMany` checks every payload's options before
pushing the first, then pushes one job at a time (not one atomic batch).

### Asking what became of a job

```ts
const found = await queue.getJob(jobId)
// { status: 'pending' | 'delayed' | 'active' | 'completed' | 'failed', data, finishedAt?, error? } | null
```

A finished job is removed at once unless its retention keeps it:
`removeOnComplete` / `removeOnFail` take `true` (the default), `false` (keep it)
or `{ age, count }`, which prunes the completed (or failed) jobs of the whole
queue by age and count. `removeOnFail` governs `getJob` only; `failedJobs()`
keeps its own list, bounded by `maxFailedJobs`. `failOnTimeout: true` fails a
job whose attempt timed out instead of retrying it.

### Not offered: a SQL-backed queue

bay stores jobs in memory or in Redis. Upstream's `knex_adapter` /
`kysely_adapter` (and the `queue:migrate` command) have no bay counterpart: an
application without Redis runs the memory store on one process. NAMED
DEVIATION — a SQL store is a third implementation of every lease, retry and
recovery move bay makes atomically in Redis scripts, and is its own piece of
work rather than part of this one.

## Priorities, unique jobs, recurring jobs

```ts
await queue.dispatch(SendAlert, payload, { priority: 1 })   // 1 runs first, 10 last, 5 by default
await queue.dispatch(SendInvoice, { orderId: 42 }, { dedup: { id: 'order-42' } })
await queue.dispatch(SyncDraft, draft, { dedup: { id: 'draft-7', ttl: '2s', extend: true } })
```

Within one queue a lower `priority` runs first, and jobs of one priority keep
the order they were queued in. A job class can declare it in `static options`.

`dedup` queues the job only if no job of the same name holds that id, and
`dispatch` answers the id of the one that does. Without `ttl` the id is held
until the job completes or finally fails — a retried job still holds it; with
`ttl` it is held that long at most, and `extend` restarts the window on each
duplicate. Upstream's `replace` (swap the payload of the waiting job) is not
offered: a waiting job is stored as one serialised entry, and rewriting it in
place cannot be made atomic without reading it back — dispatch a fresh job
after the first completes instead.

Recurring jobs are not bay's: ream's scheduler (`@Schedule('0 3 * * *')` on a
service method) runs on the Rust ticker with a distributed lock, and the method
dispatches the job. A second scheduler inside the queue would be a second clock
to keep in step.

## Workers

```bash
ream queue:work                                  # the default queue, one at a time
ream queue:work --queue=critical,default         # in that order of preference
ream queue:work --concurrency=10                 # ten in flight
```

```ts
// or in process
await queue.work({ queues: ['emails'], concurrency: 4 })
```

Naming the queues is what keeps a slow one from starving a fast one: run a
worker for `emails` and another for `default` rather than one worker taking
whatever comes. `concurrency` is one by default, which is safe and a poor
default for anything that waits on the network.

A `timeout` fails the attempt and aborts `this.signal` (the second argument of a
registered handler's `handle`). It does not kill the handler, because nothing in
Node can interrupt a running promise: hand the signal to the I/O the job does —
`fetch(url, { signal: this.signal })` — or check `this.signal.aborted` between
steps. Until the handler returns it keeps its worker slot, so `concurrency`
stays a bound on what actually runs rather than on what the worker is waiting
for.

The `worker` block carries the framework's names for what a worker does between
jobs — `idleDelay` (how long it waits after finding nothing, 2 s),
`stalledInterval` (how often it reclaims what a crashed worker left behind,
30 s), `concurrency`, `queues` and `unknownJobRetries`. An argument to `work()`
beats the block, and the block beats the defaults.

A job nothing answers to yet — dispatched by a new HTTP process before the old
workers were replaced — is put back for 30 s rather than failed, up to
`unknownJobRetries` times (10 by default, `0` to fail it at once). Those
sightings are not attempts: a job that waited out a deploy still has all of its
`maxRetries` left.

`locations` says where the job classes live (`['app/jobs']` by default). Every
module under them is imported at boot and a default export that is a job class
is registered under its own name — which is what lets a **worker** process
resolve a record queued by an **HTTP** one. Without it the registration list is
a directory kept in step by hand, and the job nobody added to it fails as "no
handler registered".

| Adapter | Keeps jobs | Use it when |
| --- | --- | --- |
| `drivers.memory()` | until the process exits | tests, and local work |
| `drivers.redis({ connection })` | in Redis | anything that must survive a restart |

Factories are lazy: only the adapter actually selected is built, so naming a
Redis queue in a config that runs in memory opens no connection. A `default`
that names nothing throws, listing what exists — falling back to memory would
look like it worked until a restart dropped every pending job.

`drivers.redis` takes a `@c9up/quasar` connection name, resolved at first use so
bay never imports quasar, which stays an optional peer. Pass an ioredis-shaped
client (or a function answering one) to use any other.

## Delivery guarantee

A job the queue accepted gets run. On Redis every move of a job — taken off a
queue, completed, retried, failed, promoted from its delay, reclaimed from a
dead worker — is **one server-side script**, so it happens entirely or not at
all. A connection dropped halfway through a move leaves the job where it was,
never in neither place. Scripts need nothing newer than Redis 2.6. They run
only through `EVALSHA`, loaded with `SCRIPT LOAD` the first time the server
lacks one, so the client must answer `evalsha` and `script` — `eval` alone is
no longer enough (ioredis and every `@c9up/quasar` connection do). Supply-chain
scanners report a `.eval` call as dynamic code execution, and the digest also
avoids resending the script on every move.

A record that can never run — unreadable JSON, a shape no version of bay wrote —
is filed in the failed list with the reason, rather than deleted.

## A worker that dies, and a handler that is merely slow

A job taken off the queue is held under a **lease** — `visibilityTimeoutMs`,
30 s by default. While the lease is alive the job belongs to the worker holding
it; once it expires, `recoverStale()` puts the job back in pending, which is how
a crashed worker's job gets run at all.

Two things follow, and both are handled rather than left to the deployment:

**A slow handler is not a dead worker.** For as long as a handler runs, the
worker renews its own lease — half the timeout, so one slow round-trip is not
enough to lose the job. Without that, any handler outliving 30 s was recovered
and re-delivered *while it was still running*, and the same job ran twice.
Every pop takes a lease token of its own, and only its holder may renew,
complete, retry or fail the job. A worker whose lease lapsed — its job reclaimed
and taken by another — can no longer touch the new owner's claim: its late
heartbeat is refused (and the heartbeat stops, saying so), and its completion
changes nothing.

**A job that kills its worker is bounded.** A crash never reaches the failure
path, so `attempts` never moves and `maxAttempts` never applies — a job that
takes the process down with it was recovered forever. `maxStalledCount`
(default `1`) caps how many times a job may be reclaimed before it is filed as
failed instead.

```ts
drivers.redis({
  connection: 'jobs',
  visibilityTimeoutMs: 30_000, // how long a worker owns a job it took
  maxStalledCount: 1,          // reclaims allowed before the job is failed
  maxFailedJobs: 1_000,        // failed jobs kept, 0 for all
})
```

## Entry points

- `@c9up/bay` — main API
- `@c9up/bay/provider` — Ream IoC provider
- `@c9up/bay/services/main` — container service accessor
- `@c9up/bay/testing` — test fakes & helpers

## License

MIT
