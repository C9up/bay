# @c9up/bay

> Pluggable job-queue contract for the Ream framework, with memory + Redis drivers.

Part of **[Ream](https://github.com/C9up/ream)** — a Rust-powered, AdonisJS-compatible Node.js framework. Independent, publishable package.

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

```ts
// start/queue.ts
import queue from '@c9up/bay/services/main'

queue.register('send-email', new SendEmailJob())
await queue.dispatch('send-email', { to: 'user@example.com' })

// A worker, with the config's `worker` block as its defaults.
await queue.work()
```

The `worker` block carries the framework's names for what a worker does between
jobs — `idleDelay` (how long it waits after finding nothing, 2 s) and
`stalledInterval` (how often it reclaims what a crashed worker left behind,
30 s). An argument to `work()` beats the block, and the block beats the
defaults.

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

## Delivery guarantee, and the one way to lose it

A job the queue accepted gets run: `pop()` moves it from pending to processing
in a single `LMOVE`, so a worker that dies mid-job leaves the job recoverable
rather than gone.

`LMOVE` needs **Redis 6.2 or later**. Without it the move is `lpop` then
`rpush`, and a crash between the two deletes the job from pending before it
reaches processing — nothing recovers it, because nothing knows it existed.
That turns at-least-once delivery into at-most-once.

So on an older Redis the driver **refuses to start in production**, and says
which two ways out there are:

```ts
// Either upgrade the server, or state that losing a job is acceptable here:
drivers.redis({ connection: 'jobs', allowNonAtomicPop: true })
```

The opt-in is honoured and still logs a warning on every process that starts
with it, naming production — agreeing once in a config file is not the same as
being reminded, in the logs of an incident, that this is how the process was
running. Outside production the fallback simply warns.

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
Renewal is refused once the lease is gone or has passed to another worker, so a
late heartbeat cannot resurrect somebody else's claim.

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
  maxFailedJobs: 1_000,        // failed jobs kept (needs LTRIM on the client)
})
```

## Entry points

- `@c9up/bay` — main API
- `@c9up/bay/provider` — Ream IoC provider
- `@c9up/bay/services/main` — container service accessor
- `@c9up/bay/testing` — test fakes & helpers

## License

MIT
