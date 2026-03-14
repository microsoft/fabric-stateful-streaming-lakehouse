# Stateful Streaming Lakehouse

A hands-on workshop for building **Spark Structured Streaming** pipelines in Microsoft Fabric — from core concepts with plain PySpark to a production-grade medallion architecture powered by [ArcFlow](https://github.com/mwc360/ArcFlow).

## 🚀 Get Started

**👉 [Install this jumpstart into your Fabric workspace](https://jumpstart.fabric.microsoft.com/catalog/stateful-streaming-lakehouse/)**

First install `fabric-jumpstart`
```python
%pip install fabric-jumpstart
```

Then:

```python
import fabric_jumpstart as jumpstart

# Install this scenario
jumpstart.install("stateful-streaming-lakehouse")
```

The jumpstart deploys everything you need — notebooks, Spark Job Definition, Eventstream, and sample data generator — directly into your workspace.