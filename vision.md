# Workshop Vision

> **Format:** Hands-on | **Level:** L300 | **Duration:** 8 hours

**Abstract:** Join this hands-on workshop to learn how a Microsoft engineering team uses Fabric Spark to tackle real-world data engineering challenges. Learn to build scalable data pipelines with Spark Structured Streaming, ML augmentation, and STAR Schema-based dimensional modeling. The workshop covers an end-to-end medallion architecture, leaving you with reusable code assets, patterns, and tips, to accelerate building your own enterprise-ready data pipelines that can be debugged, deployed, and tested.

_Workshop content w/ an added appendix of additional topics will be used for L300 partner training_

---

## Module 1 — Stateful Data Processing with Apache Spark (~90 min)

### 🔬 Lab 1: Jumpstart - Stateful Data Processing with Spark
- **Prerequisites:** Fabric Workspace
- **Objectives:**
    1. Learn streaming concepts, see it in action, understand state management

---

## Module 2 — Medallion Architecture (~90 min)

### 🔬 Lab 2: Jumpstart - Streaming Medallion Architecture
- **Prerequisites:** Fabric Workspace
- **Objectives:** Learn mature data engineering techniques — using Structured Streaming for state management, packaging ELT logic, and thinking like a software engineer. ArcFlow is the reference implementation attendees can use following the workshop, but the lessons transfer to any framework.

#### Part 1: Core Concepts with Native Spark (~30 min)
##### Context
NOTEBOOK 1: explore_streaming
```python
output_type_map={
          "shipment": "json",
          "shipment_scan_event": "kafka", # via eventstream endpoint
          "route": "parquet",
          "servicelevel": "parquet",
          "facility": "parquet",
          "exceptiontype": "parquet",
          "item": "json",
          "customer": "parquet",
          "order": "json",
      }
```
##### Milestones
1. **Why Structured Streaming for a medallion pipeline?**
    - `availableNow` trigger — batch semantics, streaming state
    - Side-by-side: batch read vs. streaming read (nearly identical code)

1. **Write streaming queries on top of files**
    - Read a file source as a stream, write to Delta with checkpoint
    - Rerun it — watch only new data process (state in action)
    - Observe a single stream: `query.lastProgress` (inputRowsPerSecond, processedRowsPerSecond, batchDuration), `query.status`

1. **Intro to Streaming API**:
    - read vs. streamRead semantics
    - triggers
    - interactive state montiroing (lastProgress, etc.)
    - awaitTermination
    - fan out -> foreachBatch

1. **Build a raw → bronze streaming pipeline in raw Spark**
    - testing input with both file sources and event store sources (i.e. Eventstream, EventHub, Kafka).
    - Challenge: how do you develop against a live stream without writing garbage to Delta?
        - Use `memory` sink + `availableNow` to inspect raw payload and validate schema
        - streaming a sample to memory
        - streaming files for testing
    - execute spark streaming job availableNow to write raw data with schema applied (from_json), snake_case normalization, audit columns

1. **Build a bronze → silver streaming pipeline in raw Spark**
    - explode, flatten, audit columns
    - execute spark streaming job availableNow to read new bronze data and apply explode, flatten, audit columns

#### Part 2: Thinking Like a Software Engineer (~30 min)
NOTEBOOK 2: arcflow_streaming_framework
_Now, ask the audience: is our Notebook code ready to put into production?_

1. **The problem with static notebook code** (slides - only lightly covered in Jumpstart Notebook Markdown)
    - The bronze transform from Part 1 works for one table — add a couple more and pain is obvious. Add a couple hundred and it's an impossible feat, both to develop consistently and to maintin.
    - What breaks: duplicated logic, no testability, no versioning, schema changes cascade

1. **Packaging your ELT framework — side-by-side comparison**
    - Introduction to ArcFlow: a couple slides on how it works
    - A slide showing conceptually highlighting all of the things which can me metadata inputs to a framework

1. **Add a new object to the framework**
    - New Notebook: this notebook is now about applying our streaming use case via a Packaged ELT framework, ArcFlow. (C:\Users\milescole\source\ArcFlow\.github\copilot-instructions.md)
        - Notebook starts with some points on static code vs packaged code
    - Same bronze → silver transform, now as a named registered transformer function
    - Same stream dev workflow (memory sink pattern), now as a reusable test method
    - Same pipeline run, now with automatic Spark job descriptions set by the framework
        - `spark.sparkContext.setJobDescription(f"Running OPTIMIZE on {table.name}")`
        - Framework design: visibility and debuggability should be built in, not bolted on
    - Same observability, now across all streams: `controller.get_status()` returns health of every running StreamingQuery in one call
    - *Learning moment: the code is nearly identical — the packaging is what changes everything*
        - Easier to test, version, distribute, and hand to a teammate
    - Guided exercise: wire up a new table — write only the business logic
        - Same bronze → silver transform, now as a named registered transformer function
        - Use ArcFlow framework (already running Spark Job Definition) and modify YAML template per two new objects to process. Validate in Notebook. Now stop and restart SJD to apply.
    - *Learning moment: the framework absorbs complexity; you're writing software, not scripts*

_Now, should we go and schedule this Notebook?_
1. **Notebooks vs. Spark Job Definitions**
    - When to graduate a notebook to a SJD

1. **Dimensional modeling**
    - Separate how SCD2/dimensional objects are built (framework) from business logic (your config)
    - Don't repeat SCD2 logic — hard to maintain, upgrade, audit


---

## Module 3 — Spark Monitoring and Optimization (~90 min)

### 🔬 Lab 3: Jumpstart - Spark Monitoring
- **Prerequisites:** Fabric Workspace
- **Objectives:** See how to monitor Spark applications

---

### 📊 Slides: Best Practices You Can't Easily Demo (~30 min)

#### Delta Lake
1. Golden configs (Always use!)
1. Concurrency control (OCC)
    - Delta uses Optimistic Concurrency Control — understand when conflicts occur vs. when they don't
    - General considerations: concurrent writers to the same table, append vs. upsert conflict zones
    - Best practices: auto compaction reduces small file accumulation that worsens conflict rates; partition design reduces conflict surface area; prefer append-only bronze, reserve merges for silver+
1. Table compaction
1. Maximize file skipping
1. Clustering / Partitioning
1. Scenario based configs
    - Optimized Write
    - V-Order
    - Extended Stats

#### Performance
1. Native Execution Engine
    - Why it's faster
1. Don't infer schema in production
1. Know your Spark configs
1. Performance tuning hierarchy (from quick wins to deep tuning concepts)

#### Orchestration
1. Ease of use and monitoring vs. complexity and high performance
---

## Module 4 — Scalable Spark Development and Deployment (~90 min)

### 🔬 Lab 4: Jumpstart - Scalable Spark Development and Deployment
- **Prerequisites:** Docker, WSL
- **Objectives:** Learn how to develop Spark applications locally using a Docker image, test, and deploy into Microsoft Fabric.


## Appendix: Extra Topics (For additional trainings)
1. Capacity best practices for Spark
1. Fbric Security considerations for Spark
1. Workspace organization for DE
