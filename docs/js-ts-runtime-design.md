# JavaScript/TypeScript Runtime for Indexify

## Overview

This document captures the design for running JavaScript/TypeScript applications on Indexify/Tensorlake. The server is already language-agnostic — the `function_executor.proto` explicitly states that messages should not use Python SDK objects, and the `InfoResponse` has `sdk_language` and `sdk_language_version` fields designed for multi-language support.

## Architecture

```
Indexify Server (Rust)  ─── gRPC ──→  Executor/Dataplane (Rust)  ─── gRPC ──→  Function Executor (Rust + V8)
       ↑                                                                              ↑
       │                                                                              │
  HTTP REST ← JS/TS Client SDK                                          JS/TS SDK (defineFunction, etc.)
```

The server and executor/dataplane remain **unchanged**. We build three new components:

1. **JS/TS Function Executor** — Rust binary with embedded V8 (via `deno_core`)
2. **JS/TS SDK** — pure TypeScript package for defining functions and applications
3. **JS/TS Client SDK** — HTTP REST client for invoking applications

---

## Execution Model: V8 Isolates (Primary) + Full Containers (Fallback)

### V8 Isolates (Default)

User code runs inside V8 isolates embedded directly in the Rust Function Executor. This is the same model used by Cloudflare Workers, Deno Deploy, and Vercel Edge Functions.

```
Executor/Dataplane ─── gRPC ──→  Rust Function Executor (tonic)
                                        │
                                        ├── Isolate Pool
                                        │     ├── V8 Isolate (function A)
                                        │     ├── V8 Isolate (function B)
                                        │     └── V8 Isolate (function C)
                                        │
                                        └── Host APIs (exposed to JS via ops)
                                              ├── blob_store.read()
                                              ├── blob_store.write()
                                              ├── console.log()
                                              ├── fetch()
                                              └── invoke_function()
```

The Function Executor is a **single Rust binary** — no Node.js, no npm, no container-per-function needed.

**Why `deno_core`:**
- Battle-tested V8 wrapper (powers Deno itself)
- Clean `op2` macro for exposing Rust functions to JS
- Handles tokio ↔ V8 event loop bridging
- ES module support built-in
- TypeScript support via SWC (add `deno_ast` for transpilation)
- V8 snapshot support for pre-compiled code (near-instant cold starts)

**Properties:**
- V8 isolate creation: ~5ms (vs ~500ms+ for a Node.js process, seconds for a container)
- With V8 snapshots (pre-compiled user code): <1ms
- Memory per isolate: ~2MB (vs ~30MB+ per Node.js process)
- Can run hundreds of concurrent isolates in a single Function Executor process
- Strong security isolation — no filesystem, no network, no child_process unless explicitly granted via ops

### Full Containers (Fallback)

For workloads that need system binaries (ffmpeg, Chromium, native npm addons), a full Node.js container is available:

```typescript
const heavyFn = defineFunction({
  name: 'transcode_video',
  runtime: 'container',
  image: new Image({ baseImage: 'node:20-slim' })
    .run('apt-get update && apt-get install -y ffmpeg'),
  handler: async (videoUrl: string) => { /* ... */ },
});
```

The selection can be automatic — if a function specifies a custom `Image`, it gets a container; otherwise it runs in an isolate.

### Comparison

| | V8 Isolate | Full Container |
|---|---|---|
| Pure JS/TS + API calls | Yes | Yes |
| Remote browser (Browserbase) | Yes | Yes |
| Local Chromium | **No** | Yes |
| ffmpeg, ImageMagick, etc. | **No** | Yes |
| Native npm addons (sharp, canvas) | **No** | Yes |
| Cold start | ~5ms | ~seconds |
| Memory overhead | ~2MB | ~50MB+ |
| Isolation | V8 sandbox | Container sandbox |

**Most AI/LLM workloads are pure API calls — isolates are the right default.**

---

## Host APIs Exposed to V8 Isolates

Each API is implemented as a Rust `op` via `deno_core`:

| API | Purpose | Rust Implementation |
|---|---|---|
| `tensorlake.blobStore.read(uri)` | Download input payloads | Blob store HTTP client |
| `tensorlake.blobStore.write(data)` | Upload output payloads | Blob store HTTP client |
| `tensorlake.invoke(fnName, args)` | Chain function calls (awaitables) | Create child allocation via gRPC |
| `console.log/warn/error` | Logging | Forward to tracing/stdout |
| `fetch()` | HTTP requests | `reqwest` under the hood |
| `setTimeout/setInterval` | Timers | Tokio timers |
| `crypto.randomUUID()` | Crypto utilities | Rust `uuid` crate |

Notably **absent** by design: `fs`, `net`, `child_process`, `require()`. Functions are pure compute with controlled I/O.

---

## Function Executor (Rust + V8)

Implements the `FunctionExecutor` gRPC service from `function_executor.proto`:

| RPC Method | Purpose | Implementation |
|---|---|---|
| `initialize()` | Load user code module from ZIP | Unzip, transpile TS via SWC, create V8 snapshot |
| `create_allocation()` | Execute a function invocation | Create isolate from snapshot, deserialize JSON inputs, call function, upload results to blob store |
| `watch_allocation_state()` | Stream execution state | Server-streaming RPC with progress, results, errors |
| `delete_allocation()` | Cancel running allocation | Terminate isolate |
| `check_health()` | Liveness probe | Simple ping |
| `get_info()` | Report SDK metadata | Return `sdk_language: "javascript"`, version |

### Example Rust code

```rust
use deno_core::{JsRuntime, RuntimeOptions, op2, extension};

#[op2(async)]
async fn op_blob_read(#[string] uri: String) -> Result<Vec<u8>, AnyError> {
    blob_store.download(&uri).await
}

#[op2(async)]
async fn op_invoke_function(
    #[string] fn_name: String,
    #[serde] args: serde_json::Value,
) -> Result<serde_json::Value, AnyError> {
    // Create a child allocation via the server
}

extension!(
    tensorlake_runtime,
    ops = [op_blob_read, op_invoke_function],
    esm_entry_point = "ext:tensorlake/runtime.js",
    esm = ["ext:tensorlake/runtime.js" = "runtime.js"],
);

async fn run_function(code: &str, fn_name: &str, input: Value) -> Result<Value> {
    let mut runtime = JsRuntime::new(RuntimeOptions {
        extensions: vec![tensorlake_runtime::init_ops_and_esm()],
        ..Default::default()
    });
    runtime.execute_script("<user>", code)?;
    let result = runtime.call_function(fn_name, input).await?;
    Ok(result)
}
```

### Proposed file structure

```
crates/
  function-executor-js/
    Cargo.toml              # depends on deno_core, tonic, proto-api
    src/
      main.rs               # Entry point (--executor-id, --address flags)
      grpc_server.rs         # tonic FunctionExecutor service impl
      isolate_pool.rs        # Pool of pre-warmed V8 isolates
      allocation_runner.rs   # Execute allocation in isolate
      ops/
        mod.rs
        blob_store.rs        # op_blob_read, op_blob_write
        invoke.rs            # op_invoke_function
        console.rs           # op_console_log
        fetch.rs             # op_fetch
      runtime.js             # JS bootstrap (globalThis.tensorlake = {...})
      typescript.rs          # SWC-based TS → JS transpilation
```

---

## JS/TS SDK

Pure TypeScript package (`@tensorlake/sdk`). Uses a builder pattern rather than decorators for simplicity and plain JS compatibility:

```typescript
import { defineFunction, defineApplication, Image } from '@tensorlake/sdk';

const getWeather = defineFunction({
  name: 'get_weather',
  cpu: 1.0,
  memory: 1.0,
  timeout: 300,
  secrets: ['WEATHER_API_KEY'],
  handler: async (city: string): Promise<WeatherData> => {
    // user code here
  },
});

const app = defineApplication({
  name: 'travel_guide',
  entrypoint: async (city: string) => {
    const weather = await getWeather.invoke(city);
    return `Guide for ${city}: ${weather.summary}`;
  },
  functions: [getWeather],
});
```

### SDK components

- **Function registry** — global map of function name → handler + metadata
- **Image builder** — generates Dockerfile instructions for Node.js base images
- **Manifest generator** — produces `ApplicationManifest` JSON matching the server's expected format
- **Awaitable/Future** — durable execution pipeline support (function chaining)
- **Type serialization** — JSON Schema generation from TypeScript types for parameter schemas

### Serialization

JSON only. No cloudpickle equivalent. Functions using JSON encoding can interop cross-language (Python ↔ JS) since the Python SDK already supports JSON serialization.

### Proposed file structure

```
packages/
  @tensorlake/sdk/
    src/
      function.ts            # defineFunction(), defineApplication()
      image.ts               # Image builder
      registry.ts            # Function registry
      manifest.ts            # ApplicationManifest generation
      client.ts              # HTTP REST client for invocation
      types.ts               # Shared types
    package.json
```

---

## JS/TS Client SDK

HTTP REST client for invoking applications from external JS/TS code:

```typescript
import { IndexifyClient } from '@tensorlake/client';

const client = new IndexifyClient({
  serverUrl: 'http://localhost:8900',
  namespace: 'default',
  apiKey: '...',
});

// Invoke and wait
const result = await client.invoke('travel_guide', { city: 'Paris' });

// Invoke with streaming progress
for await (const event of client.invokeStream('travel_guide', { city: 'Paris' })) {
  console.log(event);
}
```

### Endpoints

| Endpoint | Method | Purpose |
|---|---|---|
| `v1/namespaces/{ns}/applications` | POST | Deploy application (multipart: manifest JSON + code ZIP) |
| `v1/namespaces/{ns}/applications/{name}` | POST | Invoke application |
| `v1/namespaces/{ns}/applications/{name}/requests/{id}/progress` | GET (SSE) | Stream progress events |
| `v1/namespaces/{ns}/applications/{name}/requests/{id}/output` | GET | Get result output |

---

## CLI / Deploy Tooling

```bash
npx tensorlake deploy ./src/my_app.ts
```

Steps:
1. Load the JS/TS module
2. Discover all registered functions/applications
3. Transpile TS → JS, bundle with `esbuild` into a single file
4. For each unique `Image`, generate a Dockerfile and build
5. Create the `ApplicationManifest` JSON
6. Zip the bundle
7. Deploy via `POST /v1/namespaces/{ns}/applications`

---

## Server-Side Changes

Minimal — the server is already language-agnostic:

1. **JSON encoder support** — verify `"json"` is fully supported as `input_encoder`/`output_encoder`
2. **SDK language field** — the `InfoResponse.sdk_language` field already exists; JS Function Executor reports `"javascript"`
3. **No cloudpickle assumption** — verify no server code assumes cloudpickle encoding

---

## Use Case: Vercel AI SDK on Tensorlake

The Vercel AI SDK (`ai` package) provides excellent abstractions for LLM calls (`generateText`, `streamText`, `generateObject`, tool calling, agents). It works perfectly inside Tensorlake functions since it's pure JS making HTTP API calls.

### Example: Agent with Tools

```typescript
import { defineFunction, defineApplication } from '@tensorlake/sdk';
import { generateText, tool, stepCountIs } from 'ai';
import { anthropic } from '@ai-sdk/anthropic';
import { z } from 'zod';

const weatherAgent = defineFunction({
  name: 'weather_agent',
  cpu: 0.5, memory: 0.5, timeout: 60,
  secrets: ['ANTHROPIC_API_KEY'],
  handler: async (prompt: string): Promise<string> => {
    const { text } = await generateText({
      model: anthropic('claude-sonnet-4-5-20250514'),
      tools: {
        weather: tool({
          description: 'Get the weather in a location',
          inputSchema: z.object({ location: z.string() }),
          execute: async ({ location }) => ({ location, temperature: 72 }),
        }),
      },
      stopWhen: stepCountIs(5),
      prompt,
    });
    return text;
  },
});

export const app = defineApplication({
  name: 'weather_assistant',
  entrypoint: weatherAgent,
  functions: [weatherAgent],
});
```

### Example: Generate → Evaluate → Refine Pipeline

Each step is an independently scalable function:

```typescript
const generate = defineFunction({
  name: 'generate_copy',
  cpu: 0.5, memory: 0.5, timeout: 120,
  secrets: ['OPENAI_API_KEY'],
  handler: async (input: string): Promise<string> => {
    const { text } = await generateText({
      model: openai('gpt-4o'),
      prompt: `Write persuasive marketing copy for: ${input}`,
    });
    return text;
  },
});

const evaluate = defineFunction({
  name: 'evaluate_copy',
  cpu: 0.5, memory: 0.5, timeout: 60,
  secrets: ['OPENAI_API_KEY'],
  handler: async (copy: string) => {
    const { output } = await generateText({
      model: openai('gpt-4o'),
      output: Output.object({
        schema: z.object({
          score: z.number().min(1).max(10),
          feedback: z.array(z.string()),
        }),
      }),
      prompt: `Evaluate this marketing copy (1-10):\n${copy}`,
    });
    return { copy, ...output };
  },
});

const refine = defineFunction({
  name: 'refine_copy',
  cpu: 0.5, memory: 0.5, timeout: 120,
  secrets: ['OPENAI_API_KEY'],
  handler: async (input: { copy: string; score: number; feedback: string[] }) => {
    if (input.score >= 8) return input.copy;
    const { text } = await generateText({
      model: openai('gpt-4o'),
      prompt: `Improve based on feedback:\n${input.feedback.join('\n')}\n\nOriginal: ${input.copy}`,
    });
    return text;
  },
});

export const app = defineApplication({
  name: 'copy_pipeline',
  entrypoint: async (input: string) => {
    const copy = await generate.invoke(input);
    const evaluation = await evaluate.invoke(copy);
    return await refine.invoke(evaluation);
  },
  functions: [generate, evaluate, refine],
});
```

### Example: Parallel Fan-Out Code Review

```typescript
const securityReview = defineFunction({
  name: 'security_review',
  cpu: 0.5, memory: 0.5, timeout: 120,
  secrets: ['ANTHROPIC_API_KEY'],
  handler: async (code: string) => {
    const { output } = await generateText({
      model: anthropic('claude-sonnet-4-5-20250514'),
      output: Output.object({
        schema: z.object({
          vulnerabilities: z.array(z.string()),
          riskLevel: z.enum(['low', 'medium', 'high']),
        }),
      }),
      system: 'You are a security expert. Identify vulnerabilities.',
      prompt: `Review:\n${code}`,
    });
    return output;
  },
});

const performanceReview = defineFunction({
  name: 'performance_review',
  cpu: 0.5, memory: 0.5, timeout: 120,
  secrets: ['ANTHROPIC_API_KEY'],
  handler: async (code: string) => {
    const { output } = await generateText({
      model: anthropic('claude-sonnet-4-5-20250514'),
      output: Output.object({
        schema: z.object({
          bottlenecks: z.array(z.string()),
          optimizations: z.array(z.string()),
        }),
      }),
      system: 'You are a performance expert.',
      prompt: `Review:\n${code}`,
    });
    return output;
  },
});

const synthesize = defineFunction({
  name: 'synthesize_reviews',
  cpu: 0.5, memory: 0.5, timeout: 60,
  secrets: ['ANTHROPIC_API_KEY'],
  handler: async (reviews: any[]) => {
    const { text } = await generateText({
      model: anthropic('claude-sonnet-4-5-20250514'),
      system: 'Synthesize code reviews into actionable summary.',
      prompt: JSON.stringify(reviews, null, 2),
    });
    return text;
  },
});

export const app = defineApplication({
  name: 'code_review',
  entrypoint: async (code: string) => {
    // Fan out: run reviewers in parallel on separate containers
    const security = securityReview.invoke(code);
    const performance = performanceReview.invoke(code);
    // Fan in: synthesize
    return await synthesize.invoke([await security, await performance]);
  },
  functions: [securityReview, performanceReview, synthesize],
});
```

### Example: Stagehand/Browserbase Web Scraping

Stagehand with `env: "BROWSERBASE"` works in V8 isolates because it's just making HTTP/WebSocket calls to a remote browser — no local Chromium needed.

```typescript
import { defineFunction, defineApplication } from '@tensorlake/sdk';
import { Stagehand } from '@browserbasehq/stagehand';
import { z } from 'zod';

const scrapeFloorplans = defineFunction({
  name: 'scrape_floorplans',
  cpu: 1.0, memory: 2.0, timeout: 120,
  min_containers: 5,
  max_containers: 50,
  secrets: ['BROWSERBASE_API_KEY', 'BROWSERBASE_PROJECT_ID'],
  handler: async (url: string) => {
    const stagehand = new Stagehand({ env: 'BROWSERBASE' });
    await stagehand.init();
    const page = stagehand.page;

    await page.goto(url);
    await page.act('close pop up');
    await page.act("Click on 'Floorplans' from the navigation menu");

    const floorplans = await page.extract({
      instruction: 'Extract the floorplans from the page',
      schema: z.object({
        floorplans: z.array(z.object({
          floorplan_name: z.string(),
          floorplan_price: z.string(),
          floorplan_sqft: z.string(),
          floorplan_bedrooms: z.string(),
          floorplan_bathrooms: z.string(),
          floorplan_link: z.string(),
        })),
      }),
    });

    await stagehand.close();
    return floorplans;
  },
});

const compareListings = defineFunction({
  name: 'compare_listings',
  cpu: 0.5, memory: 1.0, timeout: 60,
  secrets: ['ANTHROPIC_API_KEY'],
  handler: async (allFloorplans: any[]) => {
    const { output } = await generateText({
      model: anthropic('claude-sonnet-4-5-20250514'),
      output: Output.object({
        schema: z.object({
          bestValue: z.string(),
          ranking: z.array(z.object({
            name: z.string(),
            score: z.number(),
            reason: z.string(),
          })),
        }),
      }),
      prompt: `Compare these apartments and rank by value:\n${JSON.stringify(allFloorplans)}`,
    });
    return output;
  },
});

export const app = defineApplication({
  name: 'apartment_research',
  entrypoint: async (urls: string[]) => {
    // Fan out: scrape N apartment sites in parallel
    const results = urls.map(url => scrapeFloorplans.invoke(url));
    const allFloorplans = await Promise.all(results);
    // Fan in: LLM compares all results
    return await compareListings.invoke(allFloorplans);
  },
  functions: [scrapeFloorplans, compareListings],
});
```

---

## Why Run Vercel AI SDK on Tensorlake

### 1. Each LLM call becomes an independently scalable unit
In vanilla AI SDK, your entire pipeline runs in one process. On Tensorlake, each `defineFunction` is separately scheduled with its own CPU, memory, and concurrency limits. Scale bottleneck steps independently.

### 2. Durable execution — survives crashes
An AI agent doing 10 tool calls takes minutes. If the process crashes on step 8, you lose everything. Tensorlake checkpoints each function result. On failure, it retries from the last successful step. With LLM calls costing real money, this avoids paying twice.

### 3. Built-in retry with backoff per step
LLM APIs are flaky — rate limits, timeouts, 500s. Tensorlake retries each function independently with configurable retry policies.

### 4. Parallel fan-out without `Promise.all` fragility
`Promise.all` in vanilla JS — if one call fails, everything fails. On Tensorlake, each parallel function is independently scheduled, retried, and tracked. Partial results are preserved.

### 5. Resource isolation between steps
An embedding step might need 2GB memory. A text generation step needs 0.5GB. On Tensorlake, each function gets exactly the resources it needs.

### 6. Secret management
API keys are injected per-function via Tensorlake's secret management. Functions only get the secrets they declare.

### 7. Cost observability per step
Tensorlake tracks execution time, resource usage, and function outcomes per step. Combined with the AI SDK's `usage` metadata (token counts), you get granular cost attribution.

### 8. Warm containers = fast cold starts
`warm_containers` and `min_containers` keep V8 isolates pre-warmed for latency-sensitive AI pipelines.

### 9. Multi-model workflows across providers
A pipeline might use Claude for generation, GPT-4o for evaluation, and a local model for embeddings. Each function can use a different provider with different API keys.

### 10. You keep the AI SDK's excellent DX
The code inside each handler is **identical** Vercel AI SDK code. You don't learn a new AI abstraction — you just wrap existing `generateText`/`streamText` calls in `defineFunction`.

---

## Implementation Phases

| Phase | Component | Effort | Dependencies |
|---|---|---|---|
| **Phase 1** | JS/TS Function Executor (Rust + deno_core + tonic) | High | Proto files from indexify repo |
| **Phase 2** | JS/TS SDK (defineFunction, defineApplication, Image, manifest) | Medium | Can parallel with Phase 1 |
| **Phase 3** | JS/TS Client SDK (HTTP REST) | Low | Can parallel |
| **Phase 4** | CLI deploy tooling (`npx tensorlake deploy`) | Medium | Phase 2 |
| **Phase 5** | Integration testing & cross-language calls | Medium | Phase 1-4 |

---

## Key Risks & Decisions

1. **npm compatibility**: V8 isolates can't run native addons. Users must bundle pure-JS deps with `esbuild`. For native addons, use the container fallback.

2. **Module loading**: Function Executor loads user code from a ZIP — extract to temp dir, transpile TS via SWC, then evaluate in isolate.

3. **Cross-language interop**: Python ↔ JS function calls work only with JSON encoding. Functions using `cloudpickle` can't interop. JSON is the natural choice for JS/TS.

4. **TypeScript compilation**: The deploy step transpiles TS → JS using `esbuild` for bundling. Alternatively, the Function Executor transpiles on `initialize()` using SWC.

5. **Package management**: The `Image.run()` method handles arbitrary commands including `npm install`. The deploy step auto-detects and includes `package.json`.
