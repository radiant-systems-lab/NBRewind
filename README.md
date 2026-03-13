# NBRewind — Artifact Evaluation

NBRewind is a checkpoint/restore system for Jupyter notebooks that enables auditing and repeating notebook executions. It consists of two components:

- **NBRewind kernel** — a custom Jupyter kernel that tracks cell execution and supports checkpoint/restore
- **taskvine_rewind** — task-level caching for distributed TaskVine/DaskVine workflows

---

## Setup

### 1. Create the conda environment

```bash
conda env create -f AE.yml --name ccgrid
conda activate ccgrid
```

### 2. Install the NBRewind kernel

```bash
cd NBRewind
python install_kernels.py
```

This registers two Jupyter kernels:
- **NBRewind** — checkpoint/restore kernel
- **NBrewind Audit Kernel** — provenance-tracking kernel (via sciunit)

### 3. Install taskvine_rewind

```bash
pip install -e .
```

---

## Running an Experiment

Each experiment is in a subdirectory under `dataset/`:

| Experiment | Directory |
|---|---|
| Climate Trend Analysis | `dataset/climate_trend/` |
| CMS Physics (dv5) | `dataset/cms-physics-dv5/` |
| Dask-TaskVine MapReduce | `dataset/dask-taskvine-mapreduce-benchmark/` |
| Distributed Image Convolution | `dataset/distributed_image_convolution/` |
| Montage | `dataset/montage/` |
| RAG Lite BM25 | `dataset/rag-lite-bm25/` |

### Steps

1. Start Jupyter:
   ```bash
   jupyter notebook
   ```

2. Navigate to the `workflow/` directory of the experiment and open the notebook.

3. Select the **NBRewind** kernel from the kernel list (Kernel → Change Kernel → NBRewind).

4. Run the notebook **top to bottom**.

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
rm -f *.pkl metadata.db rewind.txlog
```

Then re-run the notebook top to bottom with `%audit on`.

---

## Requirements

- Linux or macOS
- Conda (Miniconda or Anaconda)
- TaskVine workers (for distributed experiments) — launch with:
  ```bash
  vine_worker <manager-host> <port>
  ```
