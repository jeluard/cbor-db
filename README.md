An experimental DB abstraction layer leveraging CBOR structure and CDDL schemas.

* zero-copy (optionnaly) nested CBOR access
* compile-time nested access

## Backends

The workspace now includes these backend adapters:

* `memory` for in-process validation and baseline measurements
* [sled](https://github.com/spacejam/sled)
* [rocksdb](https://github.com/rust-rocksdb/rust-rocksdb)
* [fjall](https://github.com/fjall-rs/fjall)
* [surrealkv](https://github.com/surrealdb/surrealkv)
* [turso](https://github.com/tursodatabase/turso)
* [tidehunter](https://github.com/MystenLabs/tidehunter)

## DSL Example

The DSL is driven by the CDDL file referenced by `CBOR_DB_SCHEMA`.
With the default configuration that points to `schemas/conway.cddl`, rules such as these are available to the macros:

```cddl
block = [header]
header = [header_body]
header_body = [block_number: block_number, slot: slot, prev_hash: hash32 / nil, protocol_version]
block_number = uint .size 8
```

That schema is what lets `get!` and `update!` validate paths like `header / header_body / block_number` during compilation instead of treating them as unchecked strings.

At a high level, the DSL looks like typed path navigation over CBOR values stored in a backend:

```rust
use std::sync::Arc;

use dsl::{get, insert, update};
use store::backend::memory_backend::MemoryBackend;
use store::Store;

fn example(block_bytes: Arc<[u8]>) -> Result<(), Box<dyn std::error::Error>> {
	let store = Store::open(MemoryBackend::new())?;

	insert!(store, b"block", b"block-001", block_bytes)?;

	let block_number = get!(
		store,
		b"block",
		b"block-001",
		header / header_body / block_number
	)?;

	update!(
		store,
		b"block",
		b"block-001",
		header / header_body / block_number,
		|slot_bytes: &mut [u8]| {
			if let Some(last) = slot_bytes.last_mut() {
				*last = 0x2A;
			}
		}
	)?;

	let _full_block = store.get(b"block", b"block-001", None)?;
	let _ = block_number;
	Ok(())
}
```

Here `get!` compiles only because `block`, `header`, `header_body`, and `block_number` are all present in the configured CDDL schema and the path is valid through that schema. `update!` uses the same validation rules, but hands the closure only the byte slice for the targeted field instead of the whole value. This is especially beneficial when using a zero-copy backend.

When a path crosses a dynamic schema region, such as a choice or optional field, `dynamic = true` opts into runtime navigation while keeping the same path syntax.

## Benchmarking

Run the benchmark suite and regenerate the reports with:

```sh
make bench
```

The README benchmark snapshot is rendered by `scripts/render-benchmark-readme.js` from `docs/benchmarks/results.json`.

<!-- BENCHMARK-SNAPSHOT:START -->
Current benchmark snapshot (`make bench`, 4000 `Row` values, 24-byte keys). Each backend is benchmarked in its own child process. The workload writes the full row, reads it back with full `get!`-equivalent retrieval into a Rust struct, reads only `rewards` through `get!`, updates `rewards` to 0 through either full deserialize/mutate/re-encode or direct CBOR rewrite, then deletes the entry. The table below shows the fixed-width `row_static` path-targeted variants, resident memory before the backend run, peak observed resident memory during the run, resident memory after the backend run, and the persisted raw store size after the baseline seeded workload.

| Backend | Insert | Full get! | Partial get! | Full update! | Partial update! | Delete | RSS before / peak / after | Disk |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | --- | ---: |
| memory | 9.0M ops/s | 6.5M ops/s | 8.3M ops/s | 2.2M ops/s | 10.4M ops/s | 11.8M ops/s | 14.2 MiB / 19.5 MiB / 19.5 MiB | n/a |
| rocksdb | 355k ops/s | 1.4M ops/s | 1.6M ops/s | 266k ops/s | 295k ops/s | 408k ops/s | 14.1 MiB / 19.6 MiB / 19.6 MiB | 814 KiB |
| sled | 538k ops/s | 2.8M ops/s | 3.3M ops/s | 490k ops/s | 628k ops/s | 606k ops/s | 14.2 MiB / 41.6 MiB / 41.5 MiB | 3.1 MiB |
| surrealkv | 254k ops/s | 2.1M ops/s | 2.3M ops/s | 243k ops/s | 264k ops/s | 320k ops/s | 14.2 MiB / 216.6 MiB / 216.6 MiB | 789 KiB |
| fjall | 338k ops/s | 2.1M ops/s | 2.3M ops/s | 298k ops/s | 332k ops/s | 459k ops/s | 14.2 MiB / 19.3 MiB / 19.3 MiB | 64.0 MiB |
| tidehunter | 708k ops/s | 1.6M ops/s | 1.7M ops/s | 403k ops/s | 535k ops/s | 1.2M ops/s | 14.3 MiB / 28.7 MiB / 26.6 MiB | 6.0 MiB |
| turso | 18k ops/s | 129k ops/s | 128k ops/s | 18k ops/s | 14k ops/s | 28k ops/s | 14.1 MiB / 21.5 MiB / 20.6 MiB | 4.9 MiB |

### Backend Comparison Charts

#### Insert

```text
rocksdb    | ##############               355k ops/s
sled       | #####################        538k ops/s
surrealkv  | ##########                   254k ops/s
fjall      | #############                338k ops/s
tidehunter | ############################ 708k ops/s
turso      | #                            18k ops/s
```

#### Full get!

```text
rocksdb    | ##############               1.4M ops/s
sled       | ############################ 2.8M ops/s
surrealkv  | #####################        2.1M ops/s
fjall      | #####################        2.1M ops/s
tidehunter | ################             1.6M ops/s
turso      | #                            129k ops/s
```

#### Partial get!

```text
rocksdb    | ##############               1.6M ops/s
sled       | ############################ 3.3M ops/s
surrealkv  | ####################         2.3M ops/s
fjall      | ####################         2.3M ops/s
tidehunter | ###############              1.7M ops/s
turso      | #                            128k ops/s
```

#### Full update!

```text
rocksdb    | ###############              266k ops/s
sled       | ############################ 490k ops/s
surrealkv  | ##############               243k ops/s
fjall      | #################            298k ops/s
tidehunter | #######################      403k ops/s
turso      | #                            18k ops/s
```

#### Partial update!

```text
rocksdb    | #############                295k ops/s
sled       | ############################ 628k ops/s
surrealkv  | ############                 264k ops/s
fjall      | ###############              332k ops/s
tidehunter | ########################     535k ops/s
turso      | #                            14k ops/s
```

#### Delete

```text
rocksdb    | ##########                   408k ops/s
sled       | ###############              606k ops/s
surrealkv  | ########                     320k ops/s
fjall      | ###########                  459k ops/s
tidehunter | ############################ 1.2M ops/s
turso      | #                            28k ops/s
```
<!-- BENCHMARK-SNAPSHOT:END -->
