# @c9up/bay

> Pluggable job-queue contract for the Ream framework, with memory + Redis drivers.

Part of **[Ream](https://github.com/C9up/ream)** — a Rust-powered, AdonisJS-compatible Node.js framework. Independent, publishable package.

## Installation

```bash
pnpm add @c9up/bay
ream configure @c9up/bay
```

## Usage

Register the provider in your app, then configure it under `config/queue.ts` — the provider reads `config.get('queue')`, so a `config/bay.ts` would be loaded under the key `bay` and never seen:

```ts
// reamrc.ts
providers: [
  () => import('@c9up/bay/provider'),
]
```

## Entry points

- `@c9up/bay` — main API
- `@c9up/bay/provider` — Ream IoC provider
- `@c9up/bay/services/main` — container service accessor
- `@c9up/bay/testing` — test fakes & helpers

## License

MIT
