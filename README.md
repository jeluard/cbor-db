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
Current benchmark snapshot (`make bench`, 4000 `Row` values, 24-byte keys). Each backend is benchmarked in its own child process. The workload writes the full row, reads it back with full `get!`-equivalent retrieval into a Rust struct, reads only `rewards` through `get!`, updates `rewards` to 0 through either full deserialize/mutate/re-encode or direct CBOR rewrite, then deletes the entry. The table below shows the fixed-width `row_static` path-targeted variants and also tracks resident memory before the backend run, peak observed resident memory during the run, and resident memory after the backend run.

| Backend | Insert | Full get! | Partial get! | Full update! | Partial update! | Delete | RSS before / peak / after | Disk |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | --- | ---: |
| memory | 4.4M ops/s | 3.7M ops/s | 4.7M ops/s | 1.5M ops/s | 6.1M ops/s | 7.4M ops/s | 14.0 MiB / 18.6 MiB / 18.6 MiB | n/a |
| rocksdb | 336k ops/s | 1.4M ops/s | 1.3M ops/s | 230k ops/s | 244k ops/s | 398k ops/s | 14.0 MiB / 19.6 MiB / 19.6 MiB | 811 KiB |
| sled | 497k ops/s | 1.4M ops/s | **2.3M ops/s** | **480k ops/s** | **713k ops/s** | 559k ops/s | 14.0 MiB / 42.1 MiB / 42.1 MiB | 3.0 MiB |
| surrealkv | 283k ops/s | **2.0M ops/s** | 2.2M ops/s | 242k ops/s | 272k ops/s | 319k ops/s | 14.1 MiB / 216.4 MiB / 216.4 MiB | **789 KiB** |
| fjall | 557k ops/s | 1.7M ops/s | 1.9M ops/s | 309k ops/s | 351k ops/s | 501k ops/s | 14.1 MiB / 19.1 MiB / 19.1 MiB | 64.0 MiB |
| tidehunter | **663k ops/s** | 1.4M ops/s | 1.5M ops/s | 401k ops/s | 545k ops/s | **1.2M ops/s** | 14.1 MiB / 28.5 MiB / 26.4 MiB | 6.0 MiB |
| turso | 24k ops/s | 121k ops/s | 126k ops/s | 22k ops/s | 17k ops/s | 26k ops/s | 14.1 MiB / 21.7 MiB / 20.7 MiB | 4.9 MiB |

### Backend Comparison Charts

#### Insert

```text
rocksdb    | ##############               336k ops/s
sled       | #####################        497k ops/s
surrealkv  | ############                 283k ops/s
fjall      | ########################     557k ops/s
tidehunter | ############################ 663k ops/s
turso      | #                            24k ops/s
```

#### Full get!

```text
rocksdb    | ####################         1.4M ops/s
sled       | ####################         1.4M ops/s
surrealkv  | ############################ 2.0M ops/s
fjall      | ########################     1.7M ops/s
tidehunter | ####################         1.4M ops/s
turso      | ##                           121k ops/s
```

#### Partial get!

```text
rocksdb    | ################             1.3M ops/s
sled       | ############################ 2.3M ops/s
surrealkv  | ###########################  2.2M ops/s
fjall      | ########################     1.9M ops/s
tidehunter | ###################          1.5M ops/s
turso      | ##                           126k ops/s
```

#### Full update!

```text
rocksdb    | #############                230k ops/s
sled       | ############################ 480k ops/s
surrealkv  | ##############               242k ops/s
fjall      | ##################           309k ops/s
tidehunter | #######################      401k ops/s
turso      | #                            22k ops/s
```

#### Partial update!

```text
rocksdb    | ##########                   244k ops/s
sled       | ############################ 713k ops/s
surrealkv  | ###########                  272k ops/s
fjall      | ##############               351k ops/s
tidehunter | #####################        545k ops/s
turso      | #                            17k ops/s
```

#### Delete

```text
rocksdb    | #########                    398k ops/s
sled       | #############                559k ops/s
surrealkv  | #######                      319k ops/s
fjall      | ###########                  501k ops/s
tidehunter | ############################ 1.2M ops/s
turso      | #                            26k ops/s
```
<!-- BENCHMARK-SNAPSHOT:END -->
