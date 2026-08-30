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

## Entry points

- `@c9up/bay` — main API
- `@c9up/bay/provider` — Ream IoC provider
- `@c9up/bay/services/main` — container service accessor
- `@c9up/bay/testing` — test fakes & helpers

## License

MIT
