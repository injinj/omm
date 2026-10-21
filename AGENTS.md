# AGENTS.md — omm

OMM / RWF (Reuters "Open Message Model" over RSSL) transport: an OMM
provider (`EvOmmListen`/`EvOmmConn`, `omm_server`), an OMM consumer
(`EvOmmClient`, `omm_client`), the source directory / login / dictionary
domains (`src_dir.h`), an RV-side subscription bridge (`rv_submgr.h`),
built-in test sources (`test_pub.h`, `book_pub.h`, `test_replay.h`) and
`libommapi`. Message encoding/decoding is raimd's `rwf_msg.h`/
`rwf_writer.h`/`omm_msg.h`; this repo is the connection, state machine
and routing. Namespace `rai::omm`. Depends on raikv, raimd, sassrv,
libdecnumber. Consumed by raims (omm transport), rvcache (OMM nets),
ossl (OMM upstream) and raimdapi (`-api omm`).

## Workspace layout (injinj fleet convention)

- This repo is one of the `injinj` siblings. Every dependency is a sibling
  checkout (`../raikv`, `../raimd`, `../sassrv`, `../libdecnumber`); headers come from
  `-I../<dep>/include` and libraries from
  `../<dep>/FC43_x86_64/lib64/lib<dep>.a`. There is no install step between
  siblings — build the dependency, then build this repo.
- Dependencies are declared once, in `build_depends.mak`
  (`<dep>_dep := MAJOR.MINOR`, the rpm BuildRequires minimum). `./deps.sh`
  clones missing siblings from it, `./deps.sh -b` also builds them in
  dependency order, `./deps.sh -s` prints the commit of this repo and every
  sibling (the only record of what a release was built from).
- Whole-fleet build order lives in `~/injinj/GNUmakefile` (`make build`):
  openpgm → raikv → libdecnumber → raimd → linecook → lzf → rdbparser → h3 →
  HdrHistogram_c → sassrv → raids → natsmd → omm → raims → rvcache →
  raimdapi → ossl.

## Build

- `make` → `FC43_x86_64/bin`, `FC43_x86_64/lib64`. The build dir is
  `$(short_dist)$(lsb_dist_ver)_$(uname_m)$(port_extra)`: Fedora 43 →
  `FC43_x86_64`, Rocky 9.5 → `ROCKY9.5_x86_64`.
- `make port_extra=-g` → debug build in `FC43_x86_64-g`.
  `make port_extra=-mingw` → Windows cross build in `FC43_x86_64-mingw`
  (mingw64 toolchain; links the siblings' *master* checkouts).
- `NO_STL=1` builds with `-fno-rtti -fno-exceptions` and links with the C
  compiler; library code is written to survive that (error returns, no
  exceptions, no RTTI). Keep it that way.
- `make -j8` is safe; the makefile dependencies are complete. One trap: the
  **first `-j` run right after a compile error** can fail test executables
  with bogus undefined references (symbols from this repo's own lib)
  because they link against a library left half-updated by the failed run.
  Run plain `make` once, then `-j` works again. It is not a missing
  dependency.
- Warnings are clean. A new warning is a defect.
- If a sibling's public headers changed, rebuild the sibling first. A link
  warning like `lib<dep>.so.1.N, needed by ../<x>/.../lib<x>.so, not found`
  means sibling `<x>` was built against an older `<dep>` — rebuild `<x>`;
  at run time that warning is a hard failure.
- Ad hoc test program: capture the makefile's own lines once —
  `touch src/<any>.cpp; make -n | grep -- '-c src/<any>.cpp'` and
  `make -n | grep -- '-o FC43_x86_64/bin/' | head -1` — then reuse the
  compile line (minus `-c/-o`) and the link line's `-Wl,-rpath` list +
  `FC43_x86_64/lib64/libomm.a` + the lib tail. Run with
  `LD_LIBRARY_PATH` covering each sibling's `lib64`.
- `omm_server -t`-style built-in test sources mean a consumer test needs no
  ADS: `omm_server` publishes the RSF test service (service id 100), which
  rvcache's `test/omm.sh` and ossl's `-t` mode use.

## Versioning and release

- Version is `major_num.minor_num.patch_num-build_num` in `.copr/Makefile`
  (included by the GNUmakefile). `make spec_version` prints the
  `omm_dep := MAJOR.MINOR` line dependents pin in `build_depends.mak`.
- `rpm/omm.spec` carries placeholders (`Version: 999.999`, release
  `99999`, `BuildRequires: <dep> _<dep>_dep`); `make srpm` substitutes them
  from `build_depends.mak`. Keep the `_ensure_git` line in
  `.copr/Makefile`: COPR's `make_srpm` chroot has no git and every
  `$(shell git …)` would silently expand empty. A missing `<dep>_dep`
  entry fails the COPR build at install time, not link time.
- COPR: `injinj/dev` builds the `dev` branch of github.com/injinj/omm
  on push (webhook); `injinj/rel` builds github.com/raitechnology/omm.
  Remotes: `origin` = raitechnology (release), `ii` = injinj (dev).
  Chroots: epel-7/8/9/10, fedora-43/44/45/rawhide, x86_64.
- ABI convention: a dependent's `Requires` is `MAJOR.MINOR`, its
  `BuildRequires` the full version, and MINOR is bumped with any change to
  the C++ ABI of a public header, so a rebuilt dependent never silently
  loads an old `.so`. Bump `minor_num` here when public headers change,
  then re-pin and rebuild dependents.
- Link only against installable sibling libraries (`$(lnk_lib)`), never a
  sibling's object files; if a binary needs a sibling helper, move it into
  the sibling's library and release the sibling first.
- Signed rpms are published to https://www.raitechnology.com/repo/
  (COPR rel → download → sign → createrepo → rsync; tooling in `~/injinj/rel`).

## Code conventions

- C++11, two-space indent, `/* … */` block comments, aligned member
  comments; snake_case functions/variables, CamelCase types. System headers
  first, then project headers.
- Errors are return codes / status enums; no exceptions, no RTTI in library
  code. Raw pointers and placement-new over the repo's allocators are the
  norm; copy how neighbouring code allocates.
- Keep the generated `CMakeLists.txt` out of your edits — it is derived
  from the GNUmakefile (and slated for removal).

## Layout

- `include/omm/ev_omm.h` — provider side (listen, per-connection state:
  login → directory → dictionary → item streams).
- `ev_omm_client.h` — consumer side; what every dependent uses to pull
  items from an ADS or an `omm_server`.
- `ipc.h` — RSSL framing (ping, connect handshake, compression flags).
- `src_dir.h` — service directory model (service id/name, capabilities,
  dictionaries used/provided, QoS).
- `rv_submgr.h` — maps RV interest onto OMM item requests.
- `test_pub.h`, `book_pub.h`, `test_replay.h` — in-library test
  publishers (kept in the library on purpose so dependents' COPR builds
  can link them; see release rules).
- `ommapi.h` — the C API (`libommapi`) raimdapi links.
- bins: `omm_server`, `omm_client`, `omm_api_client`. `uploads/` holds
  captured sessions. <!-- TODO: describe uploads/ contents -->

## Contracts and gotchas

- **Envelope contract**: omm forwards whole RWF messages with
  `EvPublish.msg_enc = RWF_MSG_TYPE_ID` and
  `EvPublish.hdr_len = RwfMsgHdr.header_size + 2`. The cacheable element
  is `msg + hdr_len` (the field list). Delivery kind is
  `RwfMsgPeek::get_msg_class()` (REFRESH=2 / STATUS=3 / UPDATE=4). Every
  consumer of omm publishes (rvcache, raims) depends on this offset.
- RWF field lists carry no MSG_TYPE; the RDM dictionary
  (`RDMFieldDictionary` + `enumtype.def`) is required to iterate fields,
  so every tool takes a dictionary path.
- Stream ids are per connection and the consumer owns them; the provider
  must echo the request's stream id, and a REFRESH must be solicited
  (`RefreshFlags::SOLICITED`) only in answer to a request. Unsolicited
  refreshes are what a cache sends after a reconnect.
- `libommapi.so` is versioned against raimd; a stale build shows as
  `libraimd.so.1.N needed by libommapi.so` at link time in dependents.
  Rebuild omm after raimd bumps its minor.

## Tests

- No scripted suite. Smoke: `omm_server` with the test source, then
  `omm_client -d localhost:14002 -c <dict dir> <service>.<RIC>`.
  <!-- TODO: exact flags; rvcache/test/omm.sh is a worked example -->

## Open items / history

<!-- TODO: fill from git log -->
