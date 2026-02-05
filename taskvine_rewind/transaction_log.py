#!/usr/bin/env python3
"""
Transaction log for tracking task submissions and completions.
"""

import json
import time
import os
from threading import Lock


class TransactionLog:
    """
    Append-only transaction log for task execution history.
    """

    def __init__(self, log_file="rewind.txlog"):
        """
        Initialize transaction log.

        Args:
            log_file: Path to log file
        """
        self.log_file = log_file
        self.lock = Lock()

        # Create log file if it doesn't exist
        if not os.path.exists(log_file):
            with open(log_file, 'w') as f:
                pass  # Create empty file

    def append(self, event_type, task_id, **data):
        """
        Append an event to the log.

        Args:
            event_type: Type of event (SUBMIT, COMPLETE, FAILED)
            task_id: Unique task identifier
            **data: Additional event data
        """
        event = {
            'timestamp': time.time(),
            'event': event_type,
            'task_id': task_id,
            **data
        }
    

        # Write to log (thread-safe)
        with self.lock:
            with open(self.log_file, 'a') as f:
                f.write(json.dumps(event) + '\n')
                f.flush()
                os.fsync(f.fileno())

    def replay(self):
        """
        Replay log to reconstruct task state.

        Returns:
            dict: task_id -> task_info
        """
        tasks = {}

        if not os.path.exists(self.log_file):
            return tasks

        with open(self.log_file, 'r') as f:
            for line in f:
                if not line.strip():
                    continue

                try:
                    event = json.loads(line.strip())
                    self._apply_event(tasks, event)
                except json.JSONDecodeError:
                    # Skip corrupted lines
                    continue

        return tasks

    def _apply_event(self, tasks, event):
        """
        Apply an event to update task state.

        Args:
            tasks: Dict of task states to update
            event: Event dict

        Note on task_id usage:
        - SUBMIT events use vine_task_id (integer from TaskVine)
        - COMPLETED/FAILED events use task_hash (SHA256 content hash)

        SUBMIT events are for debugging/recovery only.
        COMPLETED events are used for cache reconstruction.
        """
        task_id = event['task_id']
        event_type = event['event']

        def _restore(value):
            if isinstance(value, list):
                return tuple(_restore(v) for v in value)
            if isinstance(value, dict):
                return {k: _restore(v) for k, v in value.items()}
            return value

        if event_type == 'SUBMIT':
            # SUBMIT events track submission metadata
            # task_id here is the vine_task_id (integer)
            tasks[task_id] = {
                'task_id': task_id,
                'command': event.get('command'),
                'status': 'SUBMITTED',
                'submit_time': event['timestamp'],
                'vine_task_id': event.get('vine_task_id'),
                # Task attributes for DaskVine
                'key': _restore(event.get('key')),
                'category': event.get('category', 'default'),
                'tag': event.get('tag'),
            }

        elif event_type == 'COMPLETED' or event_type == 'FAILED':
            # COMPLETED/FAILED events create cache entries
            # task_id here is the final task_hash (SHA256)
            # This is the stable, content-based identifier used for caching

            status = event_type
            tasks[task_id] = {
                'task_id': task_id,
                'status': status,
                'complete_time': event['timestamp'],
                'exit_code': event.get('exit_code', 0 if status == 'COMPLETED' else 1),
                'result': event.get('result'),
                'success': event.get('success', status == 'COMPLETED'),
                'std_output': event.get('std_output', ''),
                'python_output_file': event.get('python_output_file'),  # Path to cached PythonTask output
                'output_files': event.get('output_files', []),
                'input_files': event.get('input_files', []),
                # Task attributes
                'key': _restore(event.get('key')),
                'sexpr': _restore(event.get('sexpr')),
                'category': event.get('category', 'default'),
                'tag': event.get('tag'),
                'hostname': event.get('hostname', 'unknown'),
                'addrport': event.get('addrport'),
                'state': event.get('state'),
                'wall_time': event.get('wall_time', 0),
                'resources_measured': event.get('resources_measured'),
                'resources_requested': event.get('resources_requested'),
                'resources_allocated': event.get('resources_allocated'),
                'limits_exceeded': event.get('limits_exceeded'),
            }

    def clear(self):
        """
        Clear the transaction log (for testing/reset).
        """
        with self.lock:
            with open(self.log_file, 'w') as f:
                pass


if __name__ == "__main__":
    # Simple test
    print("Transaction Log - Testing")

    log = TransactionLog("test.txlog")
    log.clear()

    # Log some events
    log.append('SUBMIT', 'task_123', command='echo test', vine_task_id=1)
    log.append('COMPLETE', 'task_123', exit_code=0, result='SUCCESS')
    log.append('SUBMIT', 'task_456', command='echo test2', vine_task_id=2)
    log.append('FAILED', 'task_456', exit_code=1)

    # Replay
    tasks = log.replay()
    print(f"\nReplayed {len(tasks)} tasks:")
    for task_id, info in tasks.items():
        print(f"  {task_id[:8]}...: {info['status']} - {info['command']}")

    # Cleanup
    os.remove("test.txlog")
    print("\nTests complete!")
