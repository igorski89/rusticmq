# Original ElasticMQ Java SDK scenarios

`RusticMqSdkSuites.scala` runs the original `AmazonJavaMultiSdkTestSuite` test
traits using the Java SDK v1 and v2 clients against an external RusticMQ process
at `localhost:9321`. It replaces only server setup and queue cleanup. Assertions
and test scenarios remain those in the referenced ElasticMQ source checkout.

Use an isolated copy of the upstream tree under `target/upstream-audit`; add this
directory to that project's test source directories. Start RusticMQ with account
`123456789012`, region `elasticmq`, and base URL `http://localhost:9321`. Run both
`RusticMqSdkV1TestSuite` and `RusticMqSdkV2TestSuite` with test parallelism disabled.

The runner prepares that copy and starts the server:

```sh
brew install openjdk sbt
cargo build --locked
python3 integration/upstream/run.py ~/Work/elasticmq --nodejs
```

Port 9321 must be free. Logs and original ScalaTest reports are under
`target/upstream-audit`. The original checkout is never edited.

The upstream reference is commit `b2abbc62954851b5244211fba75c3ac83fb6e0b2`.

`--nodejs` additionally runs the original five Node.js AWS SDK tests in their
existing local-server mode. It requires Node.js/npm; omit the flag for Java only.
All 150 Java SDK tests (75 per SDK version) and five Node.js tests pass.

Outside Homebrew, set `JAVA_HOME` to your JDK. Either put `sbt` on PATH or set
`RUSTICMQ_SBT_LAUNCHER` to the absolute path of a standalone sbt launcher JAR.
CI uses JDK 21, Node.js 24 and the sbt 2.0.10 launcher with the pinned upstream
source. ScalaTest XML reports are under the isolated checkout's
`target/out/**/test-reports` directory.

To test the Docker image rather than a host executable:

```sh
python3 integration/upstream/run.py ~/Work/elasticmq --nodejs --image rusticmq:local --platform linux/arm64
```

The runner starts and stops the image on port 9321, checks graceful shutdown,
and keeps the same original scenario assertions. For the complete Rust, SDK,
protocol, restart and load audit on both Linux architectures, use
`integration/docker.py` as described in [BUILDING.md](../../BUILDING.md).
