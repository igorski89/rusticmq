# Building RusticMQ

Build from the repository root with the current stable Rust toolchain. A C
compiler is required because SQLite is compiled into the executable. There is no
separate SQLite installation or Java/Scala requirement for building or running
RusticMQ. Java and Node.js are only needed for the optional upstream SDK tests.

## macOS

Install Apple's command-line tools and Rust:

```sh
xcode-select --install
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
. "$HOME/.cargo/env"
```

Skip `xcode-select --install` if Xcode or its command-line tools are already
installed. An existing Homebrew Rust installation (`brew install rust`) also
works; choose one toolchain installation method.

Build and run, on either Apple Silicon or Intel:

```sh
cd /path/to/rusticmq
cargo build --locked --release
./target/release/rusticmq
```

For persistent storage in a data directory:

```sh
./target/release/rusticmq --data-dir ./data
```

## Linux

Install a C toolchain, curl and CA certificates. For Debian/Ubuntu:

```sh
sudo apt-get update
sudo apt-get install --yes build-essential curl ca-certificates
```

For Fedora/RHEL:

```sh
sudo dnf install gcc gcc-c++ make curl ca-certificates
```

Install Rust, then build and run:

```sh
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
. "$HOME/.cargo/env"
cd /path/to/rusticmq
cargo build --locked --release
./target/release/rusticmq --data-dir ./data
```

The native build targets your machine's architecture and libc. To distribute a
Linux executable independent of the host's libc, use the static container build
below. The bundled SQLite dependency does not require `libsqlite3-dev` or
`sqlite-devel`.

## Windows

Install Rust using the installer from [rustup.rs](https://rustup.rs/). Use the
default MSVC toolchain and install the Visual Studio prerequisites when prompted.
If installing them manually, select **Desktop development with C++** in the
Visual Studio installer, including MSVC and a Windows SDK. For Windows on ARM,
also install the ARM64 C++ build tools. See the official
[MSVC prerequisite instructions](https://rust-lang.github.io/rustup/installation/windows-msvc.html).

Open a new PowerShell window after installation so that Cargo is on PATH:

```powershell
Set-Location C:\path\to\rusticmq
cargo build --locked --release
.\target\release\rusticmq.exe
```

For persistent storage in a data directory:

```powershell
.\target\release\rusticmq.exe --data-dir .\data
```

Environment variables use PowerShell syntax:

```powershell
$env:RUSTICMQ_BIND = '127.0.0.1:9324'
$env:RUSTICMQ_BASE_URL = 'http://localhost:9324'
.\target\release\rusticmq.exe --data-dir .\data
```

## Run and verify

The default listener is `0.0.0.0:9324`; clients use `http://localhost:9324`.
Without `--data-dir DIR`, storage is in-memory SQLite (`:memory:`). The data
directory holds `rusticmq.sqlite` and its `-wal` and `-shm` files; it is created if
missing and must be writable. Stop the native server
with Ctrl+C.

While the server runs, check health in another terminal:

```sh
curl --fail http://localhost:9324/health
```

In PowerShell:

```powershell
Invoke-RestMethod http://localhost:9324/health
```

Both return `OK`. On all three operating systems, run the Rust tests with:

```sh
cargo test --locked --all-targets
```

See [README.md](README.md) for configuration and SDK integration tests. CI includes
native build/test jobs for macOS, Linux and Windows.

## Minimal Linux container

Install Docker Engine with Buildx on Linux, or Docker Desktop on macOS/Windows
using **Linux containers**. Docker builds the Linux executable, so no host Rust
installation is needed:

```sh
docker build --tag rusticmq:local .
docker run --rm --publish 9324:9324 rusticmq:local
```

The multi-stage [Dockerfile](Dockerfile) compiles against musl and uses an empty
`scratch` runtime. It contains a stripped, statically linked executable, CA
certificates for HTTPS configuration includes, license notices, a writable `/data`
directory and `/tmp`. It runs as UID/GID `10001:10001`, with no shell, package
manager, compiler or shared libraries. The `container` Cargo profile optimizes
size with LTO and aborts on panic; ordinary native release builds keep Cargo's
normal release settings.

For durable storage, use a named volume:

```sh
docker volume create rusticmq-data
docker run --rm --publish 9324:9324 --mount type=volume,source=rusticmq-data,target=/data rusticmq:local --data-dir /data
```

Alternatively, `docker compose up --build` uses the supplied named-volume setup.
Bind-mounted data directories must be writable by UID/GID `10001:10001`.
Configure `RUSTICMQ_BASE_URL` if clients connect through a different hostname or
port. `docker stop` sends SIGTERM for graceful shutdown.

The default build uses the Docker engine's architecture. To build either supported
Linux architecture explicitly:

```sh
docker buildx build --platform linux/amd64 --load --tag rusticmq:amd64 .
docker buildx build --platform linux/arm64 --load --tag rusticmq:arm64 .
```

Building a different architecture requires emulation or a matching native builder.
Docker Desktop includes emulation; see Docker's
[multi-platform build instructions](https://docs.docker.com/build/building/multi-platform/)
for Linux builder setup. Cross-architecture compilation under emulation is slower.

Inspect local image size:

```sh
docker image inspect rusticmq:local --format '{{.Size}}'
```

The result is Docker's reported local image size in bytes. With the containerd
image store, this can include both compressed layers and unpacked data; see
[Docker's disk-space accounting](https://docs.docker.com/engine/storage/containerd/#disk-space-usage).
The verified local builds report approximately **7.7 MB for ARM64** and **8.7 MB
for AMD64** after running, compared with 154 MB for the previous ARM64 Debian
runtime. Sizes depend on architecture, toolchain and image-store accounting.

To copy the static Linux executable out without running the container:

```sh
docker create --name rusticmq-extract rusticmq:local
docker cp rusticmq-extract:/usr/local/bin/rusticmq ./rusticmq-linux
docker rm rusticmq-extract
```

The extracted binary matches the image's Linux CPU architecture. The test in
`integration/container.py` checks the non-root configuration and durable SQLite
state across three container replacements without relying on tools in the image.

## Full Docker verification

The complete audit builds and tests both `linux/amd64` and `linux/arm64`, including
all 69 Rust tests in the musl builder, all 150 original Java SDK scenarios and
five original Node.js SDK tests against the runtime image, boto3 and Query/XML
workflows, container/process restarts, named-volume persistence and the concurrent
standard/FIFO load check:

```sh
python3 -m venv target/sdk-venv
target/sdk-venv/bin/pip install boto3
brew install openjdk sbt
target/sdk-venv/bin/python integration/docker.py --source ~/Work/elasticmq
```

The Homebrew command installs audit tools on macOS. Elsewhere, install a JDK and
sbt (or set `JAVA_HOME` and `RUSTICMQ_SBT_LAUNCHER` as described in
[the upstream runner guide](integration/upstream/README.md)). Node.js/npm and a
free port 9321 are also required. Add `--platform linux/amd64` or
`--platform linux/arm64` to audit just one architecture. CI runs the full audit on
native Linux runners for each architecture. Logs are in
`target/docker-audit/amd64` and `target/docker-audit/arm64`.

The Dockerfile's `test` stage is separate from its minimal runtime stage. A normal
`docker build` produces only the runtime; `integration/docker.py` explicitly
builds the test stage with `--no-cache-filter test`, so even a warm cache reruns
Rust tests. It validates the executed test counts for every suite and writes
`rust-executed.log` and `summary.json` alongside the runtime logs. `--skip-build`
explicitly skips Rust execution and records that fact in the summary.

For a receive-path benchmark at queue depths of 10,000 and 50,000:

```sh
cargo run --locked --release --example queue_benchmark -- 10000 50000
```

It measures 100 receives and 100 empty probes for standard queues and FIFO queues
with eight groups. Messages are preloaded through the restore path to isolate
selection/index costs from sending, SQLite commits and HTTP/SDK overhead. FIFO
heads become invisible during the receive workload, so it also measures blocked
groups. Results are local measurements, not throughput guarantees.

After both audits pass, publish a multi-architecture image to the local registry:

```sh
docker buildx build --platform linux/amd64,linux/arm64 --tag localhost:5555/igorski89/rusticmq:latest --tag localhost:5555/igorski89/rusticmq:0.1.0 --push .
```

Both tags point to one manifest index containing the two Linux architectures.
The registry must already be running and reachable from the Docker builder.

Rust installation reference: [Install Rust](https://rust-lang.org/install).
