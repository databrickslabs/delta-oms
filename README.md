# Delta Operational Metrics Store (DeltaOMS)
DeltaOMS is a solution providing "Automated Observability" on [Delta Lake](https://github.com/delta-io/delta)

## Project Description
This project provides a solution for automatically collecting operational metrics from Delta Lake tables into a centralized database. This will enable customers to gain operational insights and traceability around Delta Lake operations.

## Project Support
Please note that all projects in the /databrickslabs github account are provided for your exploration only, and are not formally supported by Databricks with Service Level Agreements (SLAs).  They are provided AS-IS and we do not make any guarantees of any kind.  Please do not submit a support ticket relating to any issues arising from the use of these projects.

Any issues discovered through the use of this project should be filed as GitHub Issues on the Repo.  They will be reviewed as time permits, but there are no formal SLAs for support.

## Building the Project
This scala project uses `sbt` as the build tool. Following are the high level building steps:

- `git clone` the repo to a local directory
- Execute `build/sbt clean compile` to compile the code
- Build the jar using `build/sbt clean compile assembly`
- Refer to the [build.sbt](./build.sbt) for library dependencies

## Deploying / Installing / Using the Project
Please follow the [Getting Started](./docs/GETTING%20STARTED.md) guide for instructions on using the solution.

## Releasing the Project
The solution is released as a `jar` to be used for setting up jobs. It also provides sample notebooks for analysis.Refer to the [Getting Started](./docs/GETTING%20STARTED.md) guide 

# Contributing 
We welcome contributions to DeltaOMS. See our [CONTRIBUTING.md](CONTRIBUTING.md) for more details.

# License
See [LICENSE](LICENSE).
