# NBReplay — Artifact Evaluation

NBReplay is a checkpoint/restore system for Jupyter notebooks that enables auditing and repeating notebook executions. It consists of two components:

- **NBRewind kernel** — a custom Jupyter kernel that tracks cell execution and supports checkpoint/restore
- **taskvine_rewind** — task-level caching for distributed TaskVine/DaskVine workflows

---

## Setup

### 1. Create the conda environment

```bash
conda env create -f AE.yml --name ccgrid
conda activate ccgrid
```

### 2. Install taskvine_rewind

From the root of this repository:

```bash
pip install -e .
```

### 3. Install the NBRewind kernel

```bash
cd NBRewind
python install_kernels.py
```

This registers two Jupyter kernels:
- **NBRewind** — checkpoint/restore kernel
- **NBrewind Audit Kernel** — provenance-tracking kernel (via sciunit)

---

## Running an Experiment

Each experiment is in a subdirectory under `dataset/`. Start Jupyter, open the notebook in the `workflow/` directory, select the **NBRewind** kernel (Kernel → Change Kernel → NBRewind), launch the TaskVine worker in a separate terminal, then run the notebook **top to bottom**.

> **Note for distributed clusters:** If workers are running on remote machines, ensure that port **9123** is reachable from the worker nodes to the manager host. You can verify connectivity with:
> ```bash
> nc -zv <manager-host> 9123
> ```
> On AWS EC2, open port 9123 in the instance's security group inbound rules.

### Climate Trend Analysis — `dataset/climate_trend/`

```bash
vine_worker -M ctrend
```

### CMS Physics (dv5) — `dataset/cms-physics-dv5/`

```bash
vine_worker -M cms-dv5
```

### Dask-TaskVine MapReduce Benchmark — `dataset/dask-taskvine-mapreduce-benchmark/`

```bash
vine_worker -M dask-taskvine-mapreduce-manager
```

### Distributed Image Convolution — `dataset/distributed_image_convolution/`

```bash
vine_worker -M dconv
```

### RAG Lite BM25 — `dataset/rag-lite-bm25/`

```bash
vine_worker -M rag-lite
```

---

## Audit Mode vs Repeat Mode

NBRewind operates in two modes controlled by a magic command at the top of the notebook.

### Audit Mode (first run — record checkpoints)

Add the following magic command in the first cell:

```python
%audit on
```

Run the notebook top to bottom. NBRewind will checkpoint cell outputs and track dependencies.

### Repeat Mode (subsequent runs — replay from cache)

```python
%audit off
```

In repeat mode, NBRewind replays previously checkpointed results without re-executing cells.

---

## Re-running the Audit from Scratch

To reset and perform a fresh audit, remove all checkpoint and cache files from the notebook's working directory:

```bash
rm -rf *.pkl metadata.db rewind.txlog vine_outputs/
```

Then re-run the notebook top to bottom with `%audit on`.

---

## Requirements

- Linux or macOS
- Conda (Miniconda or Anaconda)
