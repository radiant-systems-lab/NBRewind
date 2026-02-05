#!/usr/bin/env python3
"""
Wrapper library for TaskVine that provides fault-tolerant execution.

Features:
- Transaction logging of task submissions and completions
- Task state tracking and recovery
- Checkpoint/restore support (future)
"""

import os
import re
import time
import uuid
import ndcctools.taskvine as vine
from .transaction_log import TransactionLog
import cloudpickle
import inspect


_RESOURCE_FIELDS = [
    'wall_time', 'cpu_time', 'memory', 'disk', 'cores', 'gpus',
    'bandwidth', 'bytes_read', 'bytes_written', 'bytes_received', 'bytes_sent',
    'total_processes', 'max_concurrent_processes', 'start', 'end', 'swap_memory',
    'virtual_memory'
]


_UUID_SUFFIX = re.compile(r"(.*?)(-[0-9a-fA-F]{8}(?:-[0-9a-fA-F]{4}){3}-[0-9a-fA-F]{12})$")
_HASH_SUFFIX = re.compile(r"(.*?)(-[0-9a-fA-F]{12,})$")
_UUID_FILENAME = re.compile(r"^[0-9a-fA-F-]{8,}\.p$")

def _normalize_dask_label(label):
    if not isinstance(label, str):
        return label
    # strip known suffix patterns (UUID or long hex)
    m = _UUID_SUFFIX.match(label)
    if m:
        return m.group(1)
    m = _HASH_SUFFIX.match(label)
    if m:
        return m.group(1)
    return label

def collect_callables(obj):
      funcs = []
      if callable(obj):
          funcs.append(obj)
      elif isinstance(obj, (list, tuple)):
          for item in obj:
              funcs.extend(collect_callables(item))
      elif isinstance(obj, dict):
          for item in obj.values():
              funcs.extend(collect_callables(item))
      return funcs

def _canonicalize_value(value):
    if callable(value):
        label = getattr(value, "__name__", repr(value))
        return _normalize_dask_label(label)
    if isinstance(value, tuple):
        return tuple(_canonicalize_value(v) for v in value)
    if isinstance(value, list):
        return tuple(_canonicalize_value(v) for v in value)
    if isinstance(value, dict):
        canonical_items = []
        for k, v in value.items():
            ck = _canonicalize_value(k)
            if isinstance(v, str) and _UUID_FILENAME.match(v):
                cv = f"FILE_ARG:{ck}"
            else:
                cv = _canonicalize_value(v)
            canonical_items.append((ck, cv))
        canonical_items.sort(key=lambda item: repr(item[0]))
        return tuple(canonical_items)
    if isinstance(value, str):
        return _normalize_dask_label(value)
    return value


def _resources_to_dict(resources):
    """Convert TaskVine resource objects to JSON-friendly dictionaries."""
    if not resources:
        return None

    data = {}
    for field in _RESOURCE_FIELDS:
        value = getattr(resources, field, None)
        if value is not None:
            data[field] = value

    return data or None


class CachedTaskResult:
    """
    Cached task result object that mimics vine.Task interface.

    Provides full DaskVine compatibility by preserving all required attributes
    including key, sexpr, category, resources_measured, and output_file.

    Stores reference to original task object to preserve file references.
    """

    def __init__(self, task_id, original_task, cached_metadata, output_file=None):
        """
        Create a cached task result from metadata.

        Args:
            task_id: The task ID (UUID string for cached tasks)
            original_task: The original task object (for file references)
            cached_metadata: Dict with cached result data
            output_file: Pre-created vine.File object for the output (optional)
        """
        self._id = task_id
        self._original_task = original_task
        self._cached = cached_metadata
        self._output_file = output_file  # Store pre-created vine.File

    @staticmethod
    def _resource_view(resource_dict):
        class CachedResourceView:
            def __init__(self, data):
                self._data = data or {}

            def __getattr__(self, item):
                return self._data.get(item, 0)

            def __repr__(self):
                return f"CachedResourceView({self._data})"

        return CachedResourceView(resource_dict or {})

    # Standard TaskVine attributes
    @property
    def id(self):
        return self._id

    @property
    def command(self):
        return self._cached.get('command', '')

    @property
    def exit_code(self):
        return self._cached.get('exit_code', 0)

    @property
    def result(self):
        return self._cached.get('result', 0)

    @property
    def std_output(self):
        return self._cached.get('std_output', '')

    @property
    def output(self):
        """
        Get task output.

        For PythonTask, loads the Python result from the cached output file.
        For regular Task, returns std_output.
        """
        # Check if this is a cached PythonTask with output file
        python_output_file = self._cached.get('python_output_file')
        if python_output_file:
            import os
            if os.path.exists(python_output_file):
                import cloudpickle
                try:
                    with open(python_output_file, 'rb') as f:
                        return cloudpickle.load(f)
                except:
                    pass
        return self.std_output

    @property
    def output_file(self):
        """
        Get the output file object for PythonTask results.

        For cached tasks, returns the pre-created vine.File object that was
        created by Manager.declare_file() during submit().
        This allows cached results to be used as inputs to downstream tasks,
        which is required for DaskVine and task dependencies.
        """
        # Return the pre-created vine.File from Manager (created in submit())
        if self._output_file is not None:
            return self._output_file
        # Fallback: check if 
        # original task has output_file (for real executed tasks)
        if hasattr(self._original_task, 'output_file') and self._original_task.output_file:
            return self._original_task.output_file

        return None

    def successful(self):
        return self._cached.get('success', True)

    def completed(self):
        return self._cached.get('status') == 'COMPLETED'

    # DaskVine-specific attributes
    @property
    def key(self):
        """Dask graph key (if this is a DaskVine task)."""
        # Try cached metadata first, then original task
        cached_key = self._cached.get('key')

        if cached_key:
            return cached_key
        return getattr(self._original_task, 'key', None)

    @property
    def sexpr(self):
        """S-expression (for DaskVine error messages and retries)."""
        # Try to get from original task first (preserves Python objects)
        if hasattr(self._original_task, 'sexpr'):
            return self._original_task.sexpr
        # Fallback to cached string representation
        sexpr_str = self._cached.get('sexpr')
        if sexpr_str:
            try:
                return eval(sexpr_str)
            except:
                return sexpr_str
        return ()

    @property
    def category(self):
        """Task category name."""
        return self._cached.get('category', 'default')

    @property
    def hostname(self):
        """Hostname where task executed (CACHED for cached tasks)."""
        return self._cached.get('hostname', 'CACHED')

    @property
    def addrport(self):
        return self._cached.get('addrport', 'CACHED')

    @property
    def state(self):
        return self._cached.get('state', 'COMPLETED')

    @property
    def tag(self):
        """DAG tag (for DaskVine)."""
        cached_tag = self._cached.get('tag', '')
        if cached_tag:
            return cached_tag
        return getattr(self._original_task, 'tag', '')

    @property
    def resources_measured(self):
        """Resource measurements (wall_time, etc.)."""
        data = self._cached.get('resources_measured')
        if not data:
            data = {'wall_time': self._cached.get('wall_time', 0)}
        return self._resource_view(data)

    @property
    def resources_requested(self):
        data = self._cached.get('resources_requested')
        if not data:
            return None
        return self._resource_view(data)

    @property
    def resources_allocated(self):
        data = self._cached.get('resources_allocated')
        if not data:
            return None
        return self._resource_view(data)

    @property
    def limits_exceeded(self):
        limits = self._cached.get('limits_exceeded')
        if not limits:
            return None
        return self._resource_view(limits)

    # @property
    # def output_file(self):
    #     """
    #     Output file object.

    #     CRITICAL: Returns the original task's output_file to preserve
    #     the File object created during submit(). This is required for
    #     DaskVine to access cached output data.
    #     """
    #     if hasattr(self._original_task, 'output_file'):
    #         return self._original_task.output_file
    #     return None

    def load_wrapper_output(self, manager):
        """Load wrapper output (delegate to original task if available)."""
        if hasattr(self._original_task, 'load_wrapper_output'):
            return self._original_task.load_wrapper_output(manager)
        return self._cached.get('wrapper_output', None)

    def decrement_retry(self):
        """Decrement retry counter (always 0 for cached tasks)."""
        return 0

    def __repr__(self):
        key = self.key or 'unknown'
        return f"CachedTaskResult(id={self.id}, key={key}, cached=True)"


class RewindManager(vine.Manager):
    """
    Fault-tolerant TaskVine Manager with transaction logging and caching.

    Inherits from vine.Manager and adds:
    - Transaction logging of task submissions/completions
    - Task de-duplication based on command hash
    - Automatic cache replay on restart
    - Support for cached task results
    """

    def __init__(self, name=None, ports=9123, ssl=False, log_file="rewind.txlog"):
        """
        Create a new RewindManager.

        Args:
            name: Optional name for the manager
            port: Port to listen for workers (default: 9123)
            ssl: Enable SSL connections (default: False)
            log_file: Path to transaction log file (default: rewind.txlog)
        """
        # Initialize parent Manager FIRST
        super().__init__(name=name, port=ports, ssl=ssl)

        # Transaction log for persistence
        self._log = TransactionLog(log_file)

        # Task tracking
        self._task_map = {}  # vine_task_id -> task_info
        self._task_cache = {}  # task_id (command hash) -> task_info
        self._cached_results_queue = []  # Queue of cached tasks to return from wait()

        # Replay log to restore state
        self._replay_log()

    def _replay_log(self):
        """
        Replay transaction log to restore task cache state.
        """
        cached_tasks = self._log.replay()
        self._task_cache = cached_tasks

        # Print replay summary
        completed = sum(1 for t in cached_tasks.values() if t['status'] == 'COMPLETED')
        failed = sum(1 for t in cached_tasks.values() if t['status'] == 'FAILED')

        # if cached_tasks:
        #     print(f"[RewindManager] Replayed {len(cached_tasks)} tasks from log:")
        #     print(f"  - Completed: {completed}")
        #     print(f"  - Failed: {failed}")

    def submit(self, task):
        """
        Submit a task to the manager with transaction logging.

        Args:
            task: vine.Task object to submit

        Returns:
            Task ID assigned by TaskVine
        """
        # Compute task fingerprint
        # For PythonTask, this also computes and caches core_hash internally
        task_hash = task.compute_fingerprint()

        # Also get command for logging
        command = task.command if hasattr(task, 'command') else ""

        # Extract additional task attributes for comprehensive logging
        task_attributes = {}
        # if hasattr(task, 'key'):
        #     task_attributes['key'] = task.key
        # if hasattr(task, 'sexpr'):
        #     task_attributes['sexpr'] = str(task.sexpr)  # String repr for JSON
        # if hasattr(task, 'category'):
        #     task_attributes['category'] = task.category
        # if hasattr(task, 'tag'):
        #     task_attributes['tag'] = task.tag

        # Extract input file metadata from task (if it's our wrapped Task)
        input_metadata = []
        # print(f"[DEBUG] Task type: {type(task).__name__}, hasattr get_inputs: {hasattr(task, 'get_inputs')}")
        # if hasattr(task, '_tracked_inputs'):
        #     print(f"[DEBUG] task._tracked_inputs has {len(task._tracked_inputs)} items")
        # if hasattr(task, '_tracked_outputs'):
        #     print(f"[DEBUG] task._tracked_outputs has {len(task._tracked_outputs)} items")
        if hasattr(task, 'get_inputs'):
            inputs_list = task.get_inputs()
            # print(f"[DEBUG] get_inputs() returned {len(inputs_list)} items: {inputs_list}")
            for inp in inputs_list:
                file_obj = inp['file']

                # Extract info from vine.File
                source = file_obj.source() if hasattr(file_obj, 'source') else None
                file_info = {
                    'file': file_obj,  # Store file object for dependency tracking
                    'remote_name': inp['remote_name'],
                    'source': source,
                }

                input_metadata.append(file_info)

        # Extract output file metadata from task (if it's our wrapped Task)
        output_metadata = []
        # print(f"[DEBUG] hasattr get_outputs: {hasattr(task, 'get_outputs')}")
        if hasattr(task, 'get_outputs'):
            outputs_list = task.get_outputs()
            # print(f"[DEBUG] get_outputs() returned {len(outputs_list)} items: {outputs_list}")
            for out in outputs_list:
                file_obj = out['file']

                # Extract info from vine.File
                source = file_obj.source() if hasattr(file_obj, 'source') else None
                file_info = {
                    'file': file_obj,  # Store file object for dependency tracking
                    'remote_name': out['remote_name'],
                    'source': source,
                }

                output_metadata.append(file_info)

        # CRITICAL: Check for upstream dependencies before using cache
        # If any input file is an output from a currently-submitted task,
        # we CANNOT use the cache because the upstream task might produce
        # different output this run, invalidating our cached result.
        has_upstream_dependency = False

        if hasattr(task, 'get_inputs'):
            for inp in task.get_inputs():
                input_file = inp.get('file')

                # Check if this input file is an output from any pending task
                for tracked_info in self._task_map.values():
                    for tracked_output in tracked_info.get('outputs', []):
                        tracked_file = tracked_output.get('file')

                        # Compare file objects (identity check)
                        # If they're the same object, this task depends on a pending task
                        if input_file is tracked_file:
                            has_upstream_dependency = True
                            # print(f"[RewindManager] Task {task_hash[:8]}... has upstream dependency, skipping cache")
                            break

                    if has_upstream_dependency:
                        break

                if has_upstream_dependency:
                    break

        # Check cache - if task already completed AND no upstream dependencies, use cache
        if not has_upstream_dependency and task_hash in self._task_cache:
            cached = self._task_cache[task_hash]
            if cached['status'] == 'COMPLETED':
                print(f"[RewindManager] Task {task_hash[:8]}... was previously completed, using cached result")

                # NOTE: We do NOT call _finalize_outputs() for cached tasks
                # This prevents output files from being marked as PENDING,
                # which would block downstream tasks from running

                # Add to cached results queue for wait() to return
                cached_id = str(uuid.uuid4())

                # For PythonTask with cached output, declare the file so it can be used as input
                output_file_obj = None
                python_output_file = cached.get('python_output_file')
                if python_output_file and os.path.exists(python_output_file):
                    # Use Manager's declare_file() to properly create vine.File with C struct
                    output_file_obj = self.declare_file(python_output_file, cache=False)

                self._cached_results_queue.append({
                    'task': task,
                    'cached_id': cached_id,
                    'metadata': cached,
                    'output_file': output_file_obj  # Store the vine.File object
                })

                # Return cached task ID (UUID to distinguish from real TaskVine IDs)
                return cached_id

        # Not cached or failed - finalize outputs and submit to parent Manager
        # This is where we actually call add_output() on the underlying vine.Task
        if hasattr(task, '_finalize_outputs'):
            task._finalize_outputs()

        # print("Before submit")
        vine_task_id = super().submit(task)
        print(f"[DEBUG submit()] vine_task_id={vine_task_id}, task.id={task.id if hasattr(task, 'id') else 'N/A'}")

        # Log submission with input/output metadata and task attributes
        # NOTE: We use vine_task_id as the log key, NOT the task_hash.
        # The task_hash computed at submit time may have PENDING placeholders
        # and is not stable. The final, stable hash is computed at completion
        # and logged in the COMPLETED event.

        # Create serializable versions of metadata (without 'file' objects)
        # Transaction log needs JSON-serializable data only
        # print(f"[DEBUG] input_metadata before serialization: {len(input_metadata)} items")
        # print(f"[DEBUG] output_metadata before serialization: {len(output_metadata)} items")

        serializable_inputs = [
            {k: v for k, v in inp.items() if k != 'file'}
            for inp in input_metadata
        ]
        serializable_outputs = [
            {k: v for k, v in out.items() if k != 'file'}
            for out in output_metadata
        ]

        # print(f"[DEBUG] serializable_inputs: {serializable_inputs}")
        # print(f"[DEBUG] serializable_outputs: {serializable_outputs}")

        self._log.append('SUBMIT',
                         vine_task_id,  # Use vine_task_id as log key
                         command=command,
                         vine_task_id=vine_task_id,
                         inputs=serializable_inputs,
                         outputs=serializable_outputs,
                         **task_attributes)

        # Track task with input/output metadata
        self._task_map[vine_task_id] = {
            'task': task,
            'task_hash': task_hash,
            'command': command,
            'inputs': input_metadata,
            'outputs': output_metadata,
        }

        
        return vine_task_id

    def wait(self, timeout="wait_forever"):
        """
        Wait for a task to complete.

        Delegates to wait_for_tag(None, timeout) to match TaskVine's behavior.
        This ensures all caching logic is in one place and avoids duplicate code.

        Args:
            timeout: Optional timeout in seconds

        Returns:
            Completed vine.Task object (or CachedTaskResult), or None if timeout
        """
        # TaskVine's Manager.wait() internally calls wait_for_tag(None, timeout)
        # We do the same to ensure our overridden wait_for_tag() handles everything
        return self.wait_for_tag(None, timeout)

    def wait_for_tag(self, tag, timeout="wait_forever"):
        """
        Wait for a task with a specific tag to complete.

        For DaskVine compatibility, this method waits for tasks with matching tags.
        Checks cached results first, then delegates to parent Manager.

        Args:
            tag: Tag string to match
            timeout: Optional timeout in seconds (or "wait_forever")

        Returns:
            Completed vine.Task object (or CachedTaskResult), or None if timeout
        """
        import os

        # First, check if we have cached results with matching tag to return
        # Note: Our cached tasks don't currently store tags, so we return the first one
        # In a full implementation, we'd need to track tags during submit()
        if self._cached_results_queue:
            cached_entry = self._cached_results_queue.pop(0)

            print(f"[RewindManager] Returning cached result for task {cached_entry['cached_id']}")

            # Create and return cached task with original task object
            # This preserves file references (output_file) and all task attributes
            
            cached_task = CachedTaskResult(
                task_id=cached_entry['cached_id'],
                original_task=cached_entry['task'],
                cached_metadata=cached_entry['metadata'],
                output_file=cached_entry.get('output_file')  # Pass pre-created vine.File
            )

            return cached_task

        # No cached results - wait for a real task from parent Manager
        task = super().wait_for_tag(tag, timeout)

        if task:
            task_id = task.id

            # Get task info from our tracking
            if task_id in self._task_map:
                task_info = self._task_map[task_id]

                # CRITICAL: Recompute task hash now that all input files exist
                # The hash computed at submit() time may have used PENDING placeholders
                # for input files that didn't exist yet. Now we can compute the
                # definitive hash with actual file content hashes.
                #
                # For PythonTask, this will reuse the cached _core_hash and only
                # recompute input file hashes.
                original_task = task_info['task']
                final_task_hash = original_task.compute_fingerprint()

                # Use the final hash for caching (not the preliminary one)
                task_hash = final_task_hash

                # Determine status
                success = task.successful()
                status = 'COMPLETED' if success else 'FAILED'

                # Check existence of output files
                output_files_status = []
                for output in task_info.get('outputs', []):
                    source = output.get('source')
                    exists = False
                    if source:
                        exists = os.path.exists(source)

                    output_files_status.append({
                        'remote_name': output.get('remote_name'),
                        'source': source,
                        'exists': exists,
                        'file_type': output.get('file_type')
                    })

                # Check existence of input files (for debugging/validation)
                input_files_status = []
                for inp in task_info.get('inputs', []):
                    source = inp.get('source')
                    exists = False
                    if source:
                        exists = os.path.exists(source)

                    input_files_status.append({
                        'remote_name': inp.get('remote_name'),
                        'source': source,
                        'exists': exists,
                        'file_type': inp.get('file_type')
                    })

                # For PythonTask, copy output file to permanent cache directory BEFORE logging
                python_output_file = None
                if hasattr(task, 'output_file') and task.output_file:
                    try:
                        source = task.output_file.source()
                        if source and os.path.exists(source):
                            import shutil
                            os.makedirs('vine_outputs', exist_ok=True)
                            cached_path = os.path.join('vine_outputs', f'{task_hash}.output')
                            shutil.copy2(source, cached_path)
                            python_output_file = cached_path
                            print(f"[DEBUG] Copied PythonTask output to: {cached_path}")
                    except Exception as e:
                        print(f"[DEBUG] Failed to copy output file: {e}")

                addrport = getattr(task, 'addrport', None)
                state = getattr(task, 'state', None)
                resources_measured = _resources_to_dict(getattr(task, 'resources_measured', None))
                resources_requested = _resources_to_dict(getattr(task, 'resources_requested', None))
                resources_allocated = _resources_to_dict(getattr(task, 'resources_allocated', None))
                limits_exceeded = _resources_to_dict(getattr(task, 'limits_exceeded', None))

                # Log the result with comprehensive metadata (including python_output_file)
                self._log.append(status,
                                 task_hash,
                                 exit_code=task.exit_code,
                                 result=getattr(task, 'result', None),
                                 success=success,
                                 std_output=task.std_output if hasattr(task, 'std_output') else '',
                                 python_output_file=python_output_file,  # Include in transaction log
                                 output_files=output_files_status,
                                 input_files=input_files_status,
                                 # Additional task attributes
                                #  key=task.key if hasattr(task, 'key') else None,
                                #  category=task.category if hasattr(task, 'category') else None,
                                #  tag=task.tag if hasattr(task, 'tag') else tag,
                                 hostname=task.hostname if hasattr(task, 'hostname') else None,
                                 addrport=addrport,
                                 state=state,
                                 wall_time=task.resources_measured.wall_time if hasattr(task, 'resources_measured') else 0,
                                 resources_measured=resources_measured,
                                 resources_requested=resources_requested,
                                 resources_allocated=resources_allocated,
                                 limits_exceeded=limits_exceeded)

                # Update cache with comprehensive metadata
                self._task_cache[task_hash] = {
                    'task_id': task_hash,
                    'command': task_info['command'],
                    'status': status,
                    'exit_code': task.exit_code,
                    'result': getattr(task, 'result', None),
                    'success': success,
                    'std_output': task.std_output if hasattr(task, 'std_output') else '',
                    'python_output_file': python_output_file,  # Path to cached output file
                    'output_files': output_files_status,
                    'input_files': input_files_status,
                    # Additional task attributes
                    'key': task.key if hasattr(task, 'key') else None,
                    'sexpr': str(task.sexpr) if hasattr(task, 'sexpr') else None,
                    'category': task.category if hasattr(task, 'category') else 'default',
                    'tag': task.tag if hasattr(task, 'tag') else tag,
                    'hostname': task.hostname if hasattr(task, 'hostname') else 'unknown',
                    'addrport': addrport,
                    'state': state,
                    'wall_time': task.resources_measured.wall_time if hasattr(task, 'resources_measured') else 0,
                    'resources_measured': resources_measured,
                    'resources_requested': resources_requested,
                    'resources_allocated': resources_allocated,
                    'limits_exceeded': limits_exceeded,
                    'timestamp': time.time()
                }

                # Clean up task map
                del self._task_map[task_id]

        return task

    def empty(self):
        """
        Check if manager has no pending tasks.

        Returns:
            True if no tasks are waiting or running (including cached results)
        """
        # Check both cached results queue and parent manager
        return len(self._cached_results_queue) == 0 and super().empty()

    @property
    def stats(self):
        """
        Get manager statistics.

        Returns:
            vine.ManagerStats object
        """
        return super().stats

    def stats_category(self, category):
        """
        Get statistics for a specific category.

        Args:
            category: Category name

        Returns:
            Category-specific statistics
        """
        return super().stats_category(category)

    def enable_monitoring(self, watchdog=True, time_series=False):
        """
        Enable resource monitoring.

        Args:
            watchdog: Enable watchdog for resource limits
            time_series: Enable time series monitoring
        """
        return super().enable_monitoring(watchdog=watchdog, time_series=time_series)

    def declare_file(self, path, cache=False, peer_transfer=True, unlink_when_done=False):
        """
        Declare a file for caching.

        Args:
            path: Local path to file
            cache: If True or 'workflow', cache the file at workers for reuse
                   until the end of the workflow. If 'worker', the file is cache until the
                   end-of-life of the worker. If 'forever', the file is cached beyond the end-of-life of the worker.
            peer_transfer: Enable peer transfers for this file
            unlink_when_done: Delete the file when the workflow completes

        Returns:
            vine.File object
        """
        # Return bare vine.File from parent Manager
        return super().declare_file(path, cache=cache, peer_transfer=peer_transfer, unlink_when_done=unlink_when_done)

    def declare_buffer(self, buffer=None, cache=False, peer_transfer=True):
        """
        Declare a buffer for caching.

        Args:
            buffer: Buffer contents (string or bytes), or None for empty output buffer
            cache: If True or 'workflow', cache the file at workers for reuse
                   until the end of the workflow. If 'worker', the file is cache until the
                   end-of-life of the worker. If 'forever', the file is cached beyond the end-of-life of the worker.
            peer_transfer: Enable peer transfers for this file

        Returns:
            vine.File object
        """
        # Return bare vine.File from parent Manager
        return super().declare_buffer(buffer, cache=cache, peer_transfer=peer_transfer)

    def declare_url(self, url, cache=False, peer_transfer=True):
        """
        Declare a URL for download.

        Args:
            url: URL to download
            cache: If True or 'workflow', cache the file at workers for reuse
                   until the end of the workflow. If 'worker', the file is cache until the
                   end-of-life of the worker. If 'forever', the file is cached beyond the end-of-life of the worker.
            peer_transfer: Enable peer transfers for this file

        Returns:
            vine.File object
        """
        # Return bare vine.File from parent Manager
        return super().declare_url(url, cache=cache, peer_transfer=peer_transfer)

    def declare_temp(self):
        """
        Declare a temporary-like file (converted to persistent file for caching).

        CRITICAL: For caching to work, output files must:
        1. Exist at the manager side (not stay at worker)
        2. Persist across runs (not be truly temporary)

        We create a persistent file in 'vine_outputs' directory instead of
        using true temp files, but mark it with VINE_TEMP type for compatibility.

        Returns:
            vine.File object
        """
        import uuid
        import os

        # Create outputs directory if it doesn't exist
        outputs_dir = os.path.join(os.getcwd(), 'vine_outputs')
        os.makedirs(outputs_dir, exist_ok=True)

        # Generate unique filename
        filename = f"output_{uuid.uuid4().hex}.p"
        filepath = os.path.join(outputs_dir, filename)

        # Declare as regular file (will be transferred back to manager)
        # cache=False because this is an output (worker creates it)
        # unlink_when_done=False so it persists for future cache hits
        # Return bare vine.File
        return super().declare_file(filepath, cache=False, unlink_when_done=False)

    def cancel_by_taskid(self, task_id):
        """
        Cancel a task by its ID.

        Args:
            task_id: Task ID to cancel
        """
        if task_id in self._task_map:
            del self._task_map[task_id]
        return super().cancel_by_taskid(task_id)

    def __repr__(self):
        """String representation."""
        stats = super().stats
        num_workers = stats.workers_connected if stats else 0
        return f"RewindManager(name={self.name}, port={self.port}, workers={num_workers})"


# Also wrap common TaskVine classes for completeness
class Task(vine.Task):
    """
    Wrapper around vine.Task that tracks input/output files.

    This class intercepts add_input() and add_output() calls to track
    file associations for transaction logging and caching.
    """

    def __init__(self, command, **task_info):
        """
        Create a new Task.

        Args:
            command: Shell command to execute
            **task_info: Additional task parameters
        """
        # Initialize tracking lists BEFORE calling parent __init__
        # (parent might call add_input/add_output via task_info)
        self._tracked_inputs = []   # List of dicts with file, remote_name, kwargs
        self._tracked_outputs = []  # List of dicts with file, remote_name, kwargs

        # Call parent constructor
        super().__init__(command, **task_info)

    def add_input(self, file, remote_name, **kwargs):
        """
        Add an input file to the task.

        Args:
            file: File object (may be wrapped or unwrapped)
            remote_name: Name of file at worker
            **kwargs: Additional arguments (strict_input, mount_symlink, etc.)

        Returns:
            Result from parent add_input()
        """
        # Track this input
        self._tracked_inputs.append({
            'file': file,
            'remote_name': remote_name,
            'kwargs': kwargs
        })

        # Call parent add_input immediately
        # Inputs don't modify file state, so this is safe
        return super().add_input(file, remote_name, **kwargs)

    def add_output(self, file, remote_name, **kwargs):
        """
        Add an output file to the task.

        Args:
            file: File object (may be wrapped or unwrapped)
            remote_name: Name of file at worker
            **kwargs: Additional arguments (watch, failure_only, success_only)

        Returns:
            Result from parent add_output() (or None if deferred)
        """
        # Track this output
        self._tracked_outputs.append({
            'file': file,
            'remote_name': remote_name,
            'kwargs': kwargs
        })

        # DON'T call super().add_output() yet!
        # Reason: add_output() sets file.state = PENDING, which tells TaskVine
        # "this file will be created by a task". If we later return a cached
        # result (task doesn't run), the file stays PENDING forever and blocks
        # downstream tasks.
        #
        # Instead, we'll call the real add_output() in RewindManager.submit()
        # only for tasks that will actually run.
        return None

    def _finalize_outputs(self):
        """
        Internal method called by RewindManager when task will actually run.
        This adds the outputs to the underlying TaskVine task.
        """
        for out in self._tracked_outputs:
            file = out['file']
            remote_name = out['remote_name']
            kwargs = out['kwargs']

            # NOW call parent add_output with file
            super().add_output(file, remote_name, **kwargs)

    def get_inputs(self):
        """
        Get all tracked input files.

        Returns:
            List of dicts with keys: file, remote_name, kwargs
        """
        return self._tracked_inputs

    def get_outputs(self):
        """
        Get all tracked output files.

        Returns:
            List of dicts with keys: file, remote_name, kwargs
        """
        return self._tracked_outputs

    def compute_fingerprint(self):
        """
        Compute task fingerprint: command + input file contents.

        Returns:
            str: SHA256 hash of task
        """
        import hashlib
        import json
        import os

        components = [
            ('type', 'Task'),
            ('command', self.command)
        ]

        # Add input file content hashes
        input_hashes = {}
        for inp in self._tracked_inputs:
            file_obj = inp['file']
            remote_name = inp['remote_name']

            # Get file source path
            try:
                source = file_obj.source() if hasattr(file_obj, 'source') else None
            except:
                source = None

            if source and os.path.exists(source):
                # Hash file contents
                with open(source, 'rb') as f:
                    file_hash = hashlib.sha256(f.read()).hexdigest()
                input_hashes[remote_name] = file_hash
            else:
                # File doesn't exist yet - use placeholder
                input_hashes[remote_name] = f"PENDING:{id(file_obj)}"

        if input_hashes:
            components.append(('inputs', input_hashes))

        fingerprint_str = json.dumps(components, sort_keys=True)
        return hashlib.sha256(fingerprint_str.encode()).hexdigest()


class PythonTask(vine.PythonTask):
    """
    Wrapper around vine.PythonTask that tracks input/output files.

    NOTE: Unlike the Task wrapper, PythonTask does NOT use deferred outputs.

    PythonTask automatically creates and registers outputs in submit_finalize()
    via _add_inputs_outputs() (see task.py:1025). Users NEVER manually call
    add_output() on PythonTask. Therefore, we must pass through add_output()
    calls immediately to allow TaskVine's automatic output registration.

    See: vine_example_pythontask.py lines 73-77 for confirmation that users
    only create PythonTask and submit it, without calling add_output().
    """

    def __init__(self, func, *args, **kwargs):
        """
        Create a new PythonTask.

        Args:
            func: Python function to execute
            *args: Positional arguments for function
            **kwargs: Keyword arguments for function
        """
        # print(f"[DEBUG PythonTask.__init__()] START")
        # Initialize tracking lists
        self._tracked_inputs = []
        self._tracked_outputs = []
        # print(f"[DEBUG PythonTask.__init__()] Initialized tracking lists")

        # Core hash will be computed and cached at submit time
        self._core_hash = None

        # Call parent constructor
        # print(f"[DEBUG PythonTask.__init__()] Calling super().__init__()")
        super().__init__(func, *args, **kwargs)
        # print(f"[DEBUG PythonTask.__init__()] After super().__init__(), _tracked_inputs={len(self._tracked_inputs)} items")

        # Enable output caching so output files persist in TaskVine's cache
        # This is critical for RewindManager caching to work across Manager restarts
        self._cache_output = True
        # print(f"[DEBUG PythonTask.__init__()] END - _tracked_inputs={len(self._tracked_inputs)} items")

    def add_input(self, file, remote_name, **kwargs):
        """Add input file with tracking."""
        # print(f"[DEBUG PythonTask.add_input()] called with file={file}, remote_name={remote_name}")
        self._tracked_inputs.append({
            'file': file,
            'remote_name': remote_name,
            'kwargs': kwargs
        })
        # print(f"[DEBUG PythonTask.add_input()] _tracked_inputs now has {len(self._tracked_inputs)} items")

        return super().add_input(file, remote_name, **kwargs)

    def add_output(self, file, remote_name, **kwargs):
        """
        Add output file with tracking.

        CRITICAL: PythonTask automatically calls this in _add_inputs_outputs()
        (task.py:1025) during submit_finalize(). We MUST pass through to parent
        immediately to allow automatic output registration. Blocking this call
        prevents staging directory creation.
        """
        # print(f"[DEBUG PythonTask.add_output()] called with file={file}, remote_name={remote_name}")
        self._tracked_outputs.append({
            'file': file,
            'remote_name': remote_name,
            'kwargs': kwargs
        })
        # print(f"[DEBUG PythonTask.add_output()] _tracked_outputs now has {len(self._tracked_outputs)} items")

        # Pass through to parent - PythonTask needs this for automatic output
        return super().add_output(file, remote_name, **kwargs)

    def get_inputs(self):
        """Get list of tracked inputs."""
        return self._tracked_inputs

    def get_outputs(self):
        """Get list of tracked outputs."""
        return self._tracked_outputs

    def compute_core_hash(self):
        """
        Compute stable core hash: function code + arguments.

        MUST be called BEFORE submit_finalize() clears _fn_def.
        Result is cached in _core_hash.

        Returns:
            str: SHA256 hash of function + arguments
        """
        import hashlib
        import json
        import cloudpickle

        # Return cached value if available
        if self._core_hash is not None:
            return self._core_hash

        if not hasattr(self, '_fn_def') or self._fn_def is None:
            raise ValueError("compute_core_hash() must be called before submit_finalize()")

        func, args, kwargs = self._fn_def

        

        
        # print("[Rewind Debug] _fn_def args:", func)
        # print("[Rewind Debug] _fn_def args:", args)
        # print("[Rewind Debug] _fn_def kwargs:", kwargs)
        user_funcs = collect_callables(args) + collect_callables(kwargs)
        
     
        args = _canonicalize_value(args)
        kwargs = _canonicalize_value(kwargs)

        # print("[Rewind Debug] _fn_def args:", func)
        # print("[Rewind Debug] _fn_def args:", args)
        # print("[Rewind Debug] _fn_def kwargs:", kwargs)

        # print("[Rewind Debug] _fn_def args:", func)
        # print("[Rewind Debug] _fn_def args:", canonical_args)
        # print("[Rewind Debug] _fn_def kwargs:", kwargs)

        components = [('type', 'PythonTask')]

        if user_funcs:
            user_fn_hashes = []
            for func in user_funcs:
                if not callable(func):
                    continue
                try:
                    src = inspect.getsource(func).encode("utf-8")
                    user_fn_hashes.append(hashlib.sha256(src).hexdigest())
                except (OSError, TypeError):
                    continue
                
            if user_fn_hashes:
                user_fn_hashes.sort()
                components.append(("user_fns", tuple(user_fn_hashes)))
                # print(user_fn_hashes)
                
        
        # # Hash function code
        # try:
        #     func_hash = hashlib.sha256(cloudpickle.dumps(func)).hexdigest()
        #     components.append(('function', func_hash))
        # except Exception as e:
        #     # Fallback to function name
        #     func_name = getattr(func, '__name__', str(func))
        #     components.append(('function_name', func_name))

        # Hash arguments
        try:
            args_hash = hashlib.sha256(cloudpickle.dumps(args)).hexdigest()
            components.append(('args', args_hash))
        except Exception as e:
            components.append(('args_str', str(args)))

        # Hash keyword arguments
        try:
            kwargs_hash = hashlib.sha256(cloudpickle.dumps(kwargs)).hexdigest()
            components.append(('kwargs', kwargs_hash))
        except Exception as e:
            components.append(('kwargs_str', str(kwargs)))

        core_str = json.dumps(components, sort_keys=True)
        self._core_hash = hashlib.sha256(core_str.encode()).hexdigest()
        # print(core_str)
        return self._core_hash

    def compute_fingerprint(self, core_hash=None):
        """
        Compute full task fingerprint: core hash + user input files.

        Args:
            core_hash: Optional core hash (will use cached _core_hash if None)

        Returns:
            str: SHA256 hash of task
        """
        import hashlib
        import json
        import os

        # def _collect_callables(obj):
        #     funcs = []
        #     if callable(obj):
        #         funcs.append(obj)
        #     elif isinstance(obj, (list, tuple)):
        #         for item in obj:
        #             funcs.extend(_collect_callables(item))
        #     elif isinstance(obj, dict):
        #         for item in obj.values():
        #             funcs.extend(_collect_callables(item))
        #     return funcs

        if core_hash is None:
            # Use cached core hash if available, otherwise compute now
            if self._core_hash is not None:
                core_hash = self._core_hash
            else:
                core_hash = self.compute_core_hash()

        components = [('core_hash', core_hash)]

        # Add user input file hashes (exclude internal w_, f_, a_, o_ files)
        input_hashes = []
        for inp in self._tracked_inputs:
            remote_name = inp['remote_name']

            file_obj = inp['file']

            # Skip internal PythonTask files
            # if remote_name.startswith('w_') or remote_name.startswith('f_') or \
            #    remote_name.startswith('a_') or remote_name.startswith('o_'):
            # if remote_name.startswith("a_"):
            #     path = file_obj.source()
            #     if path and os.path.exists(path):
            #         with open(path, "rb") as fh:
            #             args_list, kwargs_dict = cloudpickle.load(fh)
            #             for func in _collect_callables(args_list):                        
            #                 print(hashlib.sha256(inspect.getsource(func).encode("utf-8")).hexdigest())
            #                 input_hashes.append(hashlib.sha256(inspect.getsource(func).encode("utf-8")).hexdigest())
            #                 # components.append(('user_fn', hashlib.sha256(inspect.getsource(func).encode("utf-8")).hexdigest()))
            #     continue

            if remote_name.startswith('f_') or remote_name.startswith("a_") or\
               remote_name.startswith('o_') or remote_name.startswith('w_'):
               continue
     


            # Get file source path
            try:
                source = file_obj.source() if hasattr(file_obj, 'source') else None
            except:
                source = None

            if source and os.path.exists(source):
                # Hash file contents
                with open(source, 'rb') as f:
                    file_hash = hashlib.sha256(f.read()).hexdigest()
                input_hashes.append(file_hash)
            else:
                # File doesn't exist yet - use placeholder
                input_hashes.append(f"PENDING:{id(file_obj)}")

        if input_hashes:
            input_hashes.sort()
            # print(input_hashes)
            components.append(('inputs', tuple(input_hashes)))

        # print(components)
        fingerprint_str = json.dumps(components, sort_keys=True)
        return hashlib.sha256(fingerprint_str.encode()).hexdigest()


class FunctionCall(vine.FunctionCall):
    """
    Wrapper around vine.FunctionCall that tracks input/output files.

    Provides same deferred output behavior as Task wrapper to prevent
    output files from being marked PENDING for cached tasks.
    """

    def __init__(self, library, fn, *args, **kwargs):
        """
        Create a new FunctionCall.

        Args:
            library: Library name or object
            fn: Function name to call
            *args: Positional arguments
            **kwargs: Keyword arguments
        """
        # Initialize tracking lists
        self._tracked_inputs = []
        self._tracked_outputs = []
        self._outputs_finalized = False

        # Call parent constructor
        super().__init__(library, fn, *args, **kwargs)

    def add_input(self, file, remote_name, **kwargs):
        """Add input file with tracking."""
        self._tracked_inputs.append({
            'file': file,
            'remote_name': remote_name,
            'kwargs': kwargs
        })

        return super().add_input(file, remote_name, **kwargs)

    def add_output(self, file, remote_name, **kwargs):
        """Add output file with deferred finalization."""
        self._tracked_outputs.append({
            'file': file,
            'remote_name': remote_name,
            'kwargs': kwargs
        })

        # DON'T call super().add_output() yet - defer until _finalize_outputs()
        return None

    def _finalize_outputs(self):
        """
        Finalize outputs by calling parent add_output().

        Called by RewindManager.submit() only for tasks that will actually run.
        """
        if self._outputs_finalized:
            return

        for output in self._tracked_outputs:
            super().add_output(output['file'], output['remote_name'], **output['kwargs'])

        self._outputs_finalized = True

    def get_inputs(self):
        """Get list of tracked inputs."""
        return self._tracked_inputs

    def get_outputs(self):
        """Get list of tracked outputs."""
        return self._tracked_outputs


# Re-export common classes from vine module for convenience
# Note: Constants (VINE_SCHEDULE_*, etc.) are accessed via the Manager class methods,
# not as module-level constants in the Python bindings


if __name__ == "__main__":
    # Simple test
    print("RewindManager - Pass-through wrapper for TaskVine")
    print("Creating manager...")
    m = RewindManager(name="test-manager", port=9123)
    print(f"Created: {m}")
    print(f"Manager listening on port {m.port}")
    print("\nWrapper is ready. Start implementing task submission to test.")
