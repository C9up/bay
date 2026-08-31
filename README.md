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
import { defineConfig, stores } from '@c9up/bay'
import env from '#start/env'

export default defineConfig({
  default: env.get('QUEUE_STORE'),
  stores: {
    memory: stores.memory(),
    redis:  stores.redis({ connection: 'main' }),
  },
})
```

```ts
// start/queue.ts
import queue from '@c9up/bay/services/main'

queue.register('send-email', new SendEmailJob())
await queue.dispatch('send-email', { to: 'user@example.com' })
```

| Store | Keeps jobs | Use it when |
| --- | --- | --- |
| `stores.memory()` | until the process exits | tests, and local work |
| `stores.redis({ connection })` | in Redis | anything that must survive a restart |

Factories are lazy: only the store actually selected is built, so naming a Redis
queue in a config that runs in memory opens no connection. A `default` that
names nothing throws, listing what exists — falling back to memory would look
like it worked until a restart dropped every pending job.

`stores.redis` takes a `@c9up/quasar` connection name, resolved at first use so
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
stores.redis({ connection: 'jobs', allowNonAtomicPop: true })
```

The opt-in is honoured and still logs a warning on every process that starts
with it, naming production — agreeing once in a config file is not the same as
being reminded, in the logs of an incident, that this is how the process was
running. Outside production the fallback simply warns.

## Entry points

- `@c9up/bay` — main API
- `@c9up/bay/provider` — Ream IoC provider
- `@c9up/bay/services/main` — container service accessor
- `@c9up/bay/testing` — test fakes & helpers

## License

MIT
