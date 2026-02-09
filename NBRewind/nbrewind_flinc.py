import sys
import os
from ipykernel.kernelapp import IPKernelApp
from ipykernel.ipkernel import IPythonKernel
from ipyflow import code as ipyflow_code
import re
import dill
import time
import json
import ast

from ipyflow.kernel import IPyflowKernel
from IPython import get_ipython

Threshold = 10.0 #sec

from IPython import get_ipython
from ipyflow.singletons import flow
from typing import cast

from ipyflow import code
from ipyflow import deps

import subprocess
from IPython.core.interactiveshell import InteractiveShell
from IPython.core.magic import Magics, magics_class, line_magic
import nbformat


class NamedObject:

    def __init__(self, obj_ref, code_dep):
        self.obj = obj_ref
        self.code = code_dep

import traceback
import builtins

import copy

from metadata_handler import MetadataHandler


# class VariableTracker(ast.NodeVisitor):
#     def __init__(self):
#         self.potential_accessed = set()
#         self.potential_changed = set()
#         self.global_vars = set()

#     def visit_Name(self, node):
#         self.potential_accessed.add(node.id)
#         if isinstance(node.ctx, ast.Load):
#             self.potential_accessed.add(node.id)
#         elif isinstance(node.ctx, ast.Store):
#             self.potential_accessed.add(node.id)
#         # print(node.id)
#         self.generic_visit(node)

#     def visit_Import(self, node):
#         # print(node.names)
#         for alias in node.names:
#             # print(alias.asname or alias.name)
#             self.potential_accessed.add(alias.asname or alias.name)
#             # self.global_vars.add(alias.asname or alias.name)
#         self.generic_visit(node)

#     def visit_ImportFrom(self, node):
       
#         for alias in node.names:
#             # print(alias.asname or alias.name)
#             self.potential_accessed.add(alias.asname or alias.name)
#             # self.global_vars.add(alias.asname or alias.name)
#         self.generic_visit(node)

class VariableTracker(ast.NodeVisitor):
    def __init__(self):
        self.potential_accessed = set()
        self.potential_changed = set()
        self.global_vars = set()

    def visit_Name(self, node):
        if isinstance(node.ctx, ast.Load):
            self.potential_accessed.add(node.id)
        elif isinstance(node.ctx, ast.Store):
            self.potential_accessed.add(node.id)
        self.generic_visit(node)

    def visit_Import(self, node):
        for alias in node.names:
            self.potential_accessed.add(alias.asname or alias.name)
        self.generic_visit(node)

    def visit_ImportFrom(self, node):
        for alias in node.names:
            self.potential_accessed.add(alias.asname or alias.name)
        self.generic_visit(node)

    def visit_FunctionDef(self, node):
        self.potential_accessed.add(node.name)
        self.generic_visit(node)

    def visit_AsyncFunctionDef(self, node):
        self.potential_accessed.add(node.name)
        self.generic_visit(node)

    def visit_ClassDef(self, node):
        self.potential_accessed.add(node.name)
        self.generic_visit(node)



class TrackedNamespace(dict):
    """Dict subclass to track variable accesses during cell execution"""
    def __init__(self, original_ns, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.original_ns = original_ns  # Reference to ip.user_ns
        self.accessed = set()
        self.changed = set()

    def __getitem__(self, key):
        print(f"[TrackedNamespace] Accessing key: {key}")
        print(f"[TrackedNamespace] Current keys: {list(self.keys())}")
        if key in self:
            self.accessed.add(key)
            return super().__getitem__(key)
        if key in self.original_ns:
            print(f"[TrackedNamespace] Found '{key}' in original namespace")
            self.accessed.add(key)
            return self.original_ns[key]
        if hasattr(builtins, key):
            print(f"[TrackedNamespace] Found '{key}' in builtins")
            self.accessed.add(key)
            return getattr(builtins, key)
        print(f"[TrackedNamespace] Key '{key}' not found")
        traceback.print_stack()
        raise KeyError(f"Variable '{key}' not defined")

    def __setitem__(self, key, value):
        self.accessed.add(key)
        self.changed.add(key)
        super().__setitem__(key, value)
        self.original_ns[key] = value  # Sync with original namespace

    def update(self, *args, **kwargs):
        super().update(*args, **kwargs)
        self.original_ns.update(*args, **kwargs)  # Keep original_ns in sync

    # def __getitem__(self, key):
        
        # print(f"[TrackedNamespace] Accessing key: {key}")
        # print(f"[TrackedNamespace] Current keys: {list(self.keys())}")
        # if key not in self:
        #     print(f"[TrackedNamespace] Key '{key}' not found")
        #     print("Stack trace:")
        #     traceback.print_stack()
        #     raise KeyError(f"Variable '{key}' not defined")
        # self.accessed.add(key)
        # return super().__getitem__(key)
        
        # if key == 'print':
    #         return print
    #     self.accessed.add(key)
    #     return super().__getitem__(key)

    # def __setitem__(self, key, value):
    #     self.accessed.add(key)
    #     super().__setitem__(key, value)

    # def __delitem__(self, key):
    #     # self.deleted.add(key)
    #     super().__delitem__(key)


def patch_namespace(ip):
    """Main patching function to install hooks"""
    
    def pre_run_cell(info):
        """Hook executed before each cell runs"""
        ip.tracked_ns = TrackedNamespace(ip.user_ns)
        # print(f"[Pre-run] user_ns keys: {list(ip.user_ns.keys())}")
        ip.user_ns = ip.tracked_ns  # Replace with tracked namespace

    def post_run_cell(info):
        """Hook executed after each cell completes"""
        if hasattr(ip, 'tracked_ns'):
            # Get accessed variables and process them
            accessed_vars = ip.tracked_ns.accessed
            print(f"[Kishu] Accessed variables: {accessed_vars}")

            changed = ip.tracked_ns.changed
            print(f"[Kishu] Changed variables: {changed}")
            
            # Restore original namespace (convert back to dict)
            ip.user_ns = dict(ip.tracked_ns)
            del ip.tracked_ns

    # Register the hooks
    ip.events.register('pre_run_cell', pre_run_cell)
    ip.events.register('post_run_cell', post_run_cell)

@magics_class
class NbRewindMagics(Magics):
    
    def __init__(self, kernel):
        super().__init__()
        self.kernel = kernel

    @line_magic
    def audit(self, line):
        """Enable or disable audit mode"""
        if line.strip().lower() == 'on':
            self.kernel.audit = True
            # print("Audit mode enabled")
        elif line.strip().lower() == 'off':
            self.kernel.audit = False
        #reset the workflow
        self.kernel.last_cid = 0
            # print("Audit mode disabled")
        # else:
        #     print("Usage: %audit on|off")
    
    @line_magic
    def restore(self, line):
        """Restore a checkpoint by ID"""
        # restore the checkpoint with the given id
        # if no id is given, restore the last checkpoint
        # if not line.strip():
        #     c_id = self.kernel.last_cid
        # else:
        try:
            c_id = int(line.strip())
        except ValueError:
            print("Invalid checkpoint ID. Please provide a valid integer.")
            return
        
        #path to notebook directory
        notebook = self.kernel.shell.user_ns.get("__session__", None)
        path = "/".join(notebook.split("/")[:-1])
        
        self.kernel.load_memoized_cell(path, c_id)

        # loaded_vars = {}
        # # for f in range(1, int(exec_id ) + 1):
        # self.kernel.run_checkout_command(f'{path}/vv', c_id, f'{path}/checkpoint_{c_id}.pkl')
        # fname = f'{path}/checkpoint_{c_id}.pkl'
        # if os.path.isfile(fname):
        #     with open(fname, 'rb') as dill_file:
        #         loaded_vars.update(dill.load(dill_file))
        #     # os.remove(fname)

    
        
        # # update global namespace with loaded variables
        # for var in loaded_vars:
        #     # loaded_vars[var] = loaded_vars[var]['obj']
        #     # print(loaded_vars['x']['obj'])
            
        #     self.kernel.shell.user_ns.update({var: loaded_vars[var]['obj']})

        self.kernel.last_cid = c_id

    @line_magic
    def show_all(self, line):
        # show all available checkpoints
        """Show all checkpoints"""

        if line.strip().lower() == 'next checkpoints' or line.strip().lower() == 'next checkpoint':
            notebook = self.kernel.shell.user_ns.get("__session__", None)
            notebook_path = "/".join(notebook.split("/")[:-1])
            metadata_handler = MetadataHandler(notebook_path)
            
            res = metadata_handler.get_all_checkpoints()
            
            for c_id, c in res.items():
                print("##################")
                print(f"Checkpoint ID: {c_id}")
                print(f"{c}")
                print("##################")


    @line_magic
    def show(self, line):
        # show next available checkpoints based on clast ran cell
        """Show available checkpoints"""

        if line.strip().lower() == 'next checkpoints' or line.strip().lower() == 'next checkpoint':
            notebook = self.kernel.shell.user_ns.get("__session__", None)
            notebook_path = "/".join(notebook.split("/")[:-1])
            metadata_handler = MetadataHandler(notebook_path)
            res = metadata_handler.get_next_checkpoints(self.kernel.last_cid)
            
            for c_id, c in res.items():
                print("##################")
                print(f"Checkpoint ID: {c_id}")
                print(f"{c}")
                print("##################")


class CustomKernel(IPyflowKernel):
    implementation = 'NB-Rewind'
    implementation_version = '1.0'
    language = 'python'
    language_version = sys.version.split()[0]
    language_info = {
        'name': 'python',
        'version': sys.version.split()[0],
        'mimetype': 'text/x-python',
        'file_extension': '.py',
    }
    banner = "Custom Python Kernel"
    
   
    def __init__(self, **kwargs) -> None:
        
        # Activate the patching when imported
        # self.initalized = False
        self.average_checkpoint_time = 0
        # self.times = []
        # self.checkpnt_times = []
        self.total_vars = set()
        self.nb_initialized = False
        self.diverged = False
        self.audit = False
        # get environment variable AUDIT
        # this is set in audit-kernel.json
        # if os.getenv("AUDIT", "false") == "true":
        #     self.audit = True
        #     os.unsetenv("AUDIT")
        self.last_cid = 0
        
        # if not os.path.exists(f'{os.getcwd()}/session'):
        #     os.mkdir(f'{os.getcwd()}/session')
        
        super().__init__(**kwargs)
        self.shell.register_magics(NbRewindMagics(self))
        self.shell.run_line_magic("flow", "mode normal")
        # shell = InteractiveShell.instance()
        # shell.run_line_magic("flow", "mode", "normal")
        # self.configure_ipyflow_tracer()
        # patch_namespace(self.shell)
        
        # myshell = IPyflowInteractiveShell.

    # def configure_ipyflow_tracer(self):
    #     """Patch ipyflow's tracer to use TrackedNamespace"""
    #     from ipyflow.tracing.ipyflow_tracer import IPyflowTracer
    #     original_trace = IPyflowTracer.trace

    #     def patched_trace(self, frame, event, arg):
    #         if hasattr(self.shell, 'tracked_ns'):
    #             frame.f_globals = self.shell.tracked_ns
    #         return original_trace(self, frame, event, arg)

    #     IPyflowTracer.trace = patched_trace

    def dump_namespace(self, ):
        to_dump = {}
        for k,v in self.shell.user_ns.items():
            if k in ["exit", "quit", "get_ipython"]:
                continue
            try:
                dill.dumps(v)
                to_dump[k] = v
            except (TypeError, dill.PicklingError):
                # Skip objects that cannot be pickled
                pass                

        with open(f'{os.getcwd()}/session/checkpoint_{self.shell.execution_count - 1}.pkl', 'wb') as dill_file: 
            # dill.dump_module(dill_file, self.shell)
            dill.dump(to_dump, dill_file)

    # def pre_run_cell(self, ip):
    #     """Hook executed before each cell runs"""
        
    #     ip.tracked_ns = TrackedNamespace(ip.user_ns)
    #     ip.user_ns = ip.tracked_ns  # Replace with tracked namespace
        
    # def post_run_cell(self, ip):
    #     """Hook executed after each cell completes"""
    #     if hasattr(ip, 'tracked_ns'):
    #         # Get accessed variables and process them
    #         accessed_vars = ip.tracked_ns.accessed
    #         # print(f"[Kishu] Accessed variables: {accessed_vars}")

    #         # changed = ip.tracked_ns.changed
    #         # print(f"[Kishu] Changed variables: {changed}")
            
    #         # Restore original namespace (convert back to dict)
    #         ip.user_ns = dict(ip.tracked_ns)
    #         del ip.tracked_ns

    def pre_run_cell(self, ip, code):
        ip.variable_tracker = VariableTracker()
        # Snapshot initial globals
        # ip.variable_tracker.before_globals = {
        #     k: id(ip.user_ns[k]) for k in ip.user_ns if not k.startswith('_')
        # }
        # Analyze code statically
        try:
            tree = ast.parse(code)
            ip.variable_tracker.visit(tree)
            # print(ip.variable_tracker.potential_accessed)
        except SyntaxError:
            pass

    def post_run_cell(self, ip):
        # print(ip.variable_tracker.potential_accessed)
        if hasattr(ip, 'variable_tracker'):
            # # Get changed variables via namespace comparison
            # # after_globals = {k for k in ip.user_ns if not k.startswith('_')}
            # # before_globals = set(ip.variable_tracker.before_globals.keys())
            # # changed = after_globals - before_globals 
            # # # Include variables modified in global scope or declared global
            # # changed.update(
            # #     v for v in ip.variable_tracker.potential_changed
            # #     if v in ip.user_ns and (v in ip.variable_tracker.global_vars or v in ip.user_ns)
            # # )
            # # # Get accessed variables from ipyflow's aliases and AST
            # # accessed = set()
            # # for mem in flow().aliases:
            # #     for var in flow().aliases[mem]:
            # #         if var.readable_name in ip.user_ns or hasattr(builtins, var.readable_name):
            # #             accessed.add(var.readable_name)
            # # accessed.update(
            # #     v for v in ip.variable_tracker.potential_accessed
            # #     if v in ip.user_ns or hasattr(builtins, v)
            # # )
            # globals_accessed = {k for k in ip.variable_tracker.potential_accessed if k in ip.user_ns}
            # print(f"[Kishu] Accessed variables: {globals_accessed}")
            # # print(f"[Kishu] Changed variables: {changed}")
            # # ip.variable_tracker.accessed = accessed
            # # ip.variable_tracker.changed = changed
            del ip.variable_tracker

    def getSym(self, id, name):

        if id in flow().aliases:
            for var in flow().aliases[id]:

                if var.readable_name == name:
                    return var
        
    
    def can_dill_serialize(self, obj):
        try:
            # Attempt round-trip: serialize then deserialize
            serialized = dill.dumps(obj)
            dill.loads(serialized)
            return True
        except Exception as e:
            print(f"Cannot serialize/deserialize object (type: {type(obj).__name__}): {e}")
            return False

    def get_deps(self, sym):

        lst = deps(sym)
        to_ret = []
        for dep in lst:
            to_ret.append(dep.full_path[1])
        return list(set(to_ret))
    
    def topological_sort(self, deps_graph):
        """Perform a topological sort on the dependency graph."""
        # Create a copy of the graph
        graph = {k: set(v) for k, v in deps_graph.items()}
        # Find all nodes with no dependencies
        no_deps = [k for k, v in graph.items() if not v]
        result = []
        while no_deps:
            node = no_deps.pop(0)
            result.append(node)
            # Find all nodes that depend on this one
            dependent_nodes = [k for k, v in graph.items() if node in v]
            for dependent in dependent_nodes:
                graph[dependent].remove(node)
                if not graph[dependent]:  # If no more dependencies
                    no_deps.append(dependent)
        if any(graph.values()):  # If there are remaining dependencies
            raise ValueError("Circular dependency detected")
        return result

    def clean_function_object_robust(self, func):
        """More robust function cleaning that handles multiple edge cases"""
        import types
        import copy
        
        try:
            func_copy = copy.deepcopy(func)
            original_code = func.__code__
            
            # Clean co_names (global variables)
            clean_names = tuple(name for name in original_code.co_names 
                            if not any(pattern in name for pattern in [
                                '_PYCCOLO_GUARD_',
                                '_coverage_',
                                '_pytest_',
                                '_mock_',
                                '_memory_',
                                '_line_'
                            ]))
            
            # Clean co_freevars (closure variables)
            clean_freevars = tuple(name for name in original_code.co_freevars 
                                if name not in ['self', 'cls'])  # Keep common ones
            
            # Clean co_cellvars (cell variables)
            clean_cellvars = original_code.co_cellvars  # Usually safe to keep
            
            # Create new code object
            clean_code = types.CodeType(
                original_code.co_argcount,
                original_code.co_posonlyargcount,
                original_code.co_kwonlyargcount,
                original_code.co_nlocals,
                original_code.co_stacksize,
                original_code.co_flags,
                original_code.co_code,
                original_code.co_consts,
                clean_names,
                original_code.co_varnames,
                original_code.co_filename,
                original_code.co_name,
                original_code.co_firstlineno,
                original_code.co_lnotab,
                clean_freevars,
                clean_cellvars
            )
            
            func_copy.__code__ = clean_code
            
            # Also clean default arguments if they exist
            if func.__defaults__:
                # You might need to handle default arguments separately
                pass
            
            return func_copy
        
        except Exception as e:
            # print(f"Error cleaning function {func.__name__}: {e}")
            return func

    def extract_final_assignment(self, code_str, var_name):
        """
        Extract only the final assignment statement for a given variable.

        Args:
            code_str: Full code containing multiple statements
            var_name: Variable name to extract assignment for

        Returns:
            String containing only the final assignment statement,
            or None if no assignment found
        """
        try:
            tree = ast.parse(code_str)
            lines = code_str.split('\n')

            # Find all assignments to the target variable
            assignments = []

            for node in tree.body:
                # Check different assignment types
                if isinstance(node, ast.Assign):
                    # Regular assignment: var = value
                    for target in node.targets:
                        if isinstance(target, ast.Name) and target.id == var_name:
                            # Extract the source code for this assignment
                            start_line = node.lineno - 1
                            end_line = node.end_lineno
                            assignment_code = '\n'.join(lines[start_line:end_line])
                            assignments.append(assignment_code)

                elif isinstance(node, ast.AugAssign):
                    # Augmented assignment: var += value
                    if isinstance(node.target, ast.Name) and node.target.id == var_name:
                        start_line = node.lineno - 1
                        end_line = node.end_lineno
                        assignment_code = '\n'.join(lines[start_line:end_line])
                        assignments.append(assignment_code)

                elif isinstance(node, ast.AnnAssign):
                    # Annotated assignment: var: type = value
                    if isinstance(node.target, ast.Name) and node.target.id == var_name:
                        start_line = node.lineno - 1
                        end_line = node.end_lineno
                        assignment_code = '\n'.join(lines[start_line:end_line])
                        assignments.append(assignment_code)

            # Return the last assignment found
            if assignments:
                return assignments[-1]
            else:
                print(f"Warning: No assignment found for variable '{var_name}'")
                return None

        except SyntaxError as e:
            print(f"Warning: Could not parse code for '{var_name}': {e}")
            return None

    def dump2(self, ip):
        
        """
        Dumps the current state of the kernel's user namespace to a file.
        Exclude built-in functions and objects that cannot be serialized.

        """

        # print("Dumping namespace")
        to_persist = {}
        
        exclusion_list = ["print", "display", "fake_edge_sym", "ipyflow", "flow", "aliases", "_", "exit", "quit", "get_ipython"]
        for k, obj in ip.user_ns.items():
            if k in exclusion_list or k.startswith("_"):
                continue
            # to_persist[k] = {'o'}
            
            try:
                deps = []
                if self.can_dill_serialize(self.shell.user_ns[k]):
                    obj = self.shell.user_ns[k]
                    if callable(obj):
                        obj = self.clean_function_object_robust(obj)
                else:
                    obj = None
                    deps = self.get_deps( self.getSym(id(self.shell.user_ns[k]), k))                           
                    # print(k, deps)
                    # print(f"Variable name: {k}", code( self.getSym(id(self.shell.user_ns[k]), k) ))
                to_persist[k] = {'obj': obj, 'code': code( self.getSym(id(self.shell.user_ns[k]), k) ), 'deps': deps}
                # print(to_persist)
            except Exception as e:
                # print(e)
                pass
            # if callable(obj):
            #     obj = self.clean_function_object_robust(obj)
            # to_persist[k] = {'obj': obj, 'code': None, 'deps': None}
            # try:
            #     # if self.can_dill_serialize(self.shell.user_ns[k]):
            #         # if callable(obj):
            #         #     obj = self.clean_function_object_robust(obj)
            #     # to_persist[k] = {'obj': obj, 'code': code( self.getSym(id(self.shell.user_ns[k]), k) ), 'deps': self.get_deps( self.getSym(id(self.shell.user_ns[k]), k) )}
            # except Exception :
            #     pass
            # if not self.can_dill_serialize(v):
            #     continue
        # print(to_persist)
        with open(f'{os.getcwd()}/checkpoint_{self.last_cid}.pkl', 'wb') as dill_file: 
            dill.dump(to_persist, dill_file)
       
        


    def dump1(self, ip):
        # to_persist = {}
        # self.total_vars.update({k for k in ip.variable_tracker.potential_accessed if k in ip.user_ns})
        # for var in self.total_vars:
        #     obj = None
        #     # if var.is_module:
        #     #     obj = var.obj
        #     # else:
        #     if self.can_dill_serialize(self.shell.user_ns[var]):
        #         obj = self.shell.user_ns[var]
        #     to_persist[var] = {'obj': obj, 'code': code( self.getSym(id(self.shell.user_ns[var]), var) ), 'deps': self.get_deps( self.getSym(id(self.shell.user_ns[var]), var) )}

        # with open(f'{os.getcwd()}/checkpoint_{self.last_cid}.pkl', 'wb') as dill_file: 
        #     # dill.dump_module(dill_file, self.shell)
        #     dill.dump(to_persist, dill_file)
        # print(self.total_vars)

        exclusion_list = ["print", "display", "fake_edge_sym", "ipyflow", "flow", "aliases", "_"]
        globals_accessed = {k for k in ip.variable_tracker.potential_accessed if k in ip.user_ns}
        # print(globals_accessed)
        
        to_persist = {}
        co_variables = []  # List of sets, where each set contains connected variables
        
        # Go through all memory locations
        entries_to_process = []
        for _, entrySet in flow().aliases.items():
            entries_to_process.append(entrySet)
        # for _, entrySet in flow().aliases.items():
        old_limit = sys.getrecursionlimit()
        sys.setrecursionlimit(50)  # Very low limit
        # readable_name = entry.readable_name
        
        for entrySet in entries_to_process:
            # Create set of variables for current memory location
            varSet = set()
            for entry in entrySet:
                try:
                    # print(f"Processing entry: {entry}")
                    if hasattr(entry, 'full_path') and len(entry.full_path) <= 1:
                        continue

                    if entry.readable_name.startswith("<literal_sym_"):
                                continue
                    if "__ipyflow_mutation" in entry.readable_name:
                        continue
                    
                    if not entry.is_module and not entry.is_anonymous:
                        var = entry.full_path[1]

                        if var in exclusion_list:
                            continue

                        if var:
                            varSet.add(var)
                except RecursionError as e:
                    pass
                    # print(f"RecursionError for entry ID: {id(entry)}")
                    # safe_attrs = ['is_module', 'is_anonymous']
                    # for attr in safe_attrs:
                    #     if hasattr(entry, attr):
                    #         try:
                    #             print(f"{attr}: {getattr(entry, attr)}")
                    #         except:
                    #             print(f"Cannot access {attr}")
            
            # If no valid variables found, skip
            if not varSet:
                continue
                
            new_co_variables = []
            merged_set = varSet
            # print("varSet", merged_set)
            for s in co_variables:
                if len(s & merged_set) == 0:
                    ## this is not affected, add this to the result
                    new_co_variables.append(s)
                else:
                    merged_set |= s
                    # print("merged", merged_set)
            new_co_variables.append(merged_set)
            co_variables = new_co_variables
        sys.setrecursionlimit(old_limit)
        
        ## go over globals and persist only co_variables that were affected
        for g in globals_accessed:
            for co_var in co_variables:
                if g not in co_var:
                    continue

                ## persist co_variable
                for var in co_var:
                    obj = None
                    deps = []
                    try:
                        if self.can_dill_serialize(self.shell.user_ns[var]):
                            obj = self.shell.user_ns[var]
                            if callable(obj):
                                obj = self.clean_function_object_robust(obj)
                        else:
                            deps = self.get_deps( self.getSym(id(self.shell.user_ns[var]), var))                           
                        # print(var, obj)
                        to_persist[var] = {'obj': obj, 'code': code( self.getSym(id(self.shell.user_ns[var]), var) ), 'deps': deps}
                    except Exception :
                        pass

        with open(f'{os.getcwd()}/checkpoint_{self.last_cid}.pkl', 'wb') as dill_file: 
            dill.dump(to_persist, dill_file)
       
        
    def dump(self, ip):
            exclusion_list = ["print", "display", "fake_edge_sym", "ipyflow", "flow", "aliases", "_"]
            globals_accessed = {k for k in ip.variable_tracker.potential_accessed if k in ip.user_ns}
            
            to_persist = {}
            # Keep track of previous size to detect changes
            prev_size = 0
            
            # Iterate until globals_accessed stops growing
            while len(globals_accessed) != prev_size:
                prev_size = len(globals_accessed)
                
                # Create a copy of the keys to avoid modification during iteration
                # memory_locations = list(flow().aliases.keys())
                
                # Go through all memory references
                for mem in flow().aliases:
                    entrySet = flow().aliases[mem]
                    varSet = []
                    
                    # Check if any of the variables in this memory location are in globals_accessed
                    should_add = False
                    for entry in entrySet:
                        if entry.readable_name.startswith("<literal_sym_"):
                            continue
                        if "__ipyflow_mutation" in entry.readable_name:
                            continue
                        
                        if not entry.is_module and not entry.is_anonymous:
                            var = entry.full_path[1]
                            if var in exclusion_list:
                                continue
                            
                            if var:
                                varSet.append(var)
                                if var in globals_accessed:
                                    should_add = True
                    
                    # If any variable in this memory location is in globals_accessed,
                    # add all other variables from this memory location
                    if should_add:
                        for var in varSet:
                            obj = None
                            # if var.is_module:
                            #     obj = var.obj
                            # else:
                            if self.can_dill_serialize(self.shell.user_ns[var]):
                                obj = self.shell.user_ns[var]
                            to_persist[var] = {'obj': obj, 'code': code( self.getSym(id(self.shell.user_ns[var]), var) ), 'deps': self.get_deps( self.getSym(id(self.shell.user_ns[var]), var) )}
                        globals_accessed.update(varSet)

            with open(f'{os.getcwd()}/checkpoint_{self.last_cid}.pkl', 'wb') as dill_file: 
                # dill.dump_module(dill_file, self.shell)
                dill.dump(to_persist, dill_file)
            print(globals_accessed)
                

        # globals_accessed = {k for k in ip.variable_tracker.potential_accessed if k in ip.user_ns}
        # exclusion_list = ["print", "display", "fake_edge_sym", "ipyflow", "flow", "aliases", "_"]
        # to_persist = {}
        # # print(ip.tracked_ns.accessed)
        # for mem in flow().aliases:
        #     entrySet = flow().aliases[mem]
        #     varSet = []
        #     should_persist = False
        #     for entry in entrySet:
                
        #         var = None
        #         if entry.readable_name.startswith("<literal_sym_"):
        #             continue
        #         if "__ipyflow_mutation" in entry.readable_name:
        #             continue
                
        #         # print(entry, entry.full_path)
        #         # print(entry, entry.is_module)
                
        #         if not entry.is_module and not entry.is_anonymous:
        #             var = entry.full_path[1]
        #         # if not entry.is_anonymous:
        #         #     var = entry.full_path[1]  
        #         if var in exclusion_list:
        #             continue
               
        #         # print(entry.full_path)
        #         if var:
        #             varSet.append(var)
        #         if var in globals_accessed: 
        #             should_persist = True

                
            
                # print(varSet, entrySet)
                # for var in varSet:
                #     if should_persist:
                #         obj = None
                
                # for var in globals_accessed:
                #     if var.is_module:
                #         obj = var.obj
                #     # else:
                #     if self.can_dill_serialize(self.shell.user_ns[var]):
                #         obj = self.shell.user_ns[var]                    
                #     to_persist[var] = {'obj': obj, 'code': code( self.getSym(id(self.shell.user_ns[var]), var) ), 'deps': self.get_deps( self.getSym(id(self.shell.user_ns[var]), var) )}
                            #  NamedObject(self.shell.user_ns[var], code( self.getSym(id(self.shell.user_ns[var]), var) ))

        # for var in to_persist.keys():
        #     if var == 'obj1':
        #         # print(to_persist)
        #         print(code( self.getSym(id(self.shell.user_ns[var]), var) ))
                    
                
        # print("Persisted Variables: ", to_persist.keys())
        # with open(f'{os.getcwd()}/checkpoint_{self.last_cid}.pkl', 'wb') as dill_file: 
        #     # dill.dump_module(dill_file, self.shell)
        #     # print(to_persist)
        #     dill.dump(to_persist, dill_file)

    def run_commit_command(self, path_to_binary, exec_id, path_to_file):
        """
        Generates and runs the command: ./vv commit {exec_id} {path_to_file}
        
        :param exec_id: The execution ID to include in the command
        :param path_to_file: The path to the .pkl file
        """
        if not os.path.isfile(path_to_file):
            print(f"The file {path_to_file} does not exist.")
            # raise FileNotFoundError(f"The file {path_to_file} does not exist.")
            return
        
        # remove json file if it already exists
        if os.path.exists(f"{os.getcwd()}/{exec_id}.json"):
            os.remove(f"{os.getcwd()}/{exec_id}.json")
                
        # Generate the command
        command = f"{path_to_binary} commit {exec_id} {path_to_file}"
        
        # Run the command
        try:
            result = subprocess.run(command, shell=True, check=True, text=True, capture_output=True)
            # print("Command executed successfully:")
            os.remove(path_to_file)
            # open json file so that sciunit can capture it.
            with open(f'{exec_id}.json', 'r') as f:
                f.read()
            # print(result.stdout)
        except subprocess.CalledProcessError as e:
            print("Error while executing the command:")
            print(e.stderr)
    
    def run_checkout_command(self, path_to_binary, exec_id, path_to_file):
        """
        Generates and runs the command: ./vv checkout {exec_id} {path_to_file}
        
        :param path_to_binary: The path to the vv executable
        :param exec_id: The execution ID to include in the command
        :param path_to_file: The path where the file should be checked out to
        """
        # Check if the directory for the output file exists
        output_dir = os.path.dirname(path_to_file)
        if output_dir and not os.path.isdir(output_dir):
            raise FileNotFoundError(f"The directory {output_dir} does not exist for {path_to_file}.")
        
        # Generate the command
        command = f"{path_to_binary} checkout {exec_id} {path_to_file}"
        
        # Run the command
        try:
            result = subprocess.run(command, shell=True, check=True, text=True, capture_output=True)
            # print("Checkout command executed successfully:")
            # print(result.stdout)
        except subprocess.CalledProcessError as e:
            # print("Error while executing the checkout command:")
            # print(e.stderr)
            pass

    def persist_metadata(self, path, code):
        
        ## Get hash of code 
        ## Persist hash of code and execution counter in an sqlite database
        try:
            metadata_handler = MetadataHandler(path)
            # c_id = metadata_handler.get_checkpoint(code, self.last_cid)
            # if not c_id:
            c_id = metadata_handler.persist_metadata(code, self.last_cid)
            self.last_cid = c_id
            # print(self.last_cid)

        except Exception as e:
            traceback.print_exc()
            print(f"Error persisting metadata: {e}")


    
    def load_memoized_cell(self, path, c_id):
        # gets execution id associated with the code
        # load data from from the persisted session files till the execution id starting from 0
            
        loaded_vars = {}
        # for f in range(1, int(exec_id ) + 1):
        self.run_checkout_command(f'{path}/vv', c_id, f'{path}/checkpoint_{c_id}.pkl')
        fname = f'{path}/checkpoint_{c_id}.pkl'
        if os.path.isfile(fname):
            with open(fname, 'rb') as dill_file:
                x = dill.load(dill_file)
                # print(x)
                loaded_vars.update(x)
            # os.remove(fname)

    
        
        # update global namespace with loaded variables
        # print(loaded_vars)
        for var in loaded_vars:
            # loaded_vars[var] = loaded_vars[var]['obj']
            # print(loaded_vars[var]['obj'])
            self.shell.user_ns.update({var: loaded_vars[var]['obj']})

        # Build dependency graph
        deps_graph = {var: data['deps'] for var, data in loaded_vars.items() if data['obj'] is None}

        # Get execution order using topological sort
        # execution_order = self.topological_sort(deps_graph)
        # print(execution_order)

        # recreated_list = [var for var in loaded_vars.items() if data['obj'] is None]
        namespace = self.shell.user_ns

        # self.shell.user_ns['A'] = None

        # print(namespace['A'])
        # Execute code for each variable in order, only if missing
        for var_name in deps_graph:
            # Check if the variable exists and is not None in the namespace
            # print(namespace['A'])
            if var_name in namespace and namespace[var_name] is not None:
                continue  # Skip if the object already exists and is not None

            # print(loaded_vars[var_name])
            full_code = str(loaded_vars[var_name]['code'])

            try:
                # Extract only the final assignment statement
                final_assignment = self.extract_final_assignment(full_code, var_name)

                # print(var_name)
                if final_assignment:
                    # print(f"Restoring '{var_name}' with final assignment only:")
                    # print(final_assignment)
                    exec(final_assignment, namespace)
                else:
                    # Fallback: if we can't extract assignment, run full code
                    # print(f"Warning: Falling back to full code execution for '{var_name}'")
                    # print(full_code)
                    exec(full_code, namespace)

            except Exception as e:
                print(f"Error restoring {var_name}: {str(e)}")
                continue
            # print(f"Restored checkpoint {f}")                        
                    
            # else:
            #     continue
            #     print("No checkpoint found for the given ID")
    

    # @contextmanager
    # def capture_iopub(self):
    #     queue = SimpleQueue()

    #     original_send = self.send_response

    #     def capturing_send(socket, msg_type, content, parent=None, ident=None, buffers=None):
    #         print("capturing")
    #         if msg_type in ("execute_result", "stream", "error", "display_data"):
    #             queue.put((msg_type, content))
    #         original_send(socket, msg_type, content, parent, ident, buffers)

    #     self.send_response = capturing_send
    #     try:
    #         yield queue
    #     finally:
    #         self.send_response = original_send

    # @contextmanager
    # def capture_ipython_outputs(self):
    #     captured = []
    #     orig_publish = self.shell.display_pub.publish

    #     def wrapped_publish(data, metadata=None, source=None, transient=None, update=False):
    #         # Capture IPython output before it goes to frontend
    #         print("capturing")
    #         captured.append({
    #             "output_type": "display_data",
    #             "data": copy.deepcopy(data),
    #             "metadata": copy.deepcopy(metadata or {})
    #         })
    #         return orig_publish(data, metadata, source, transient, update)

    #     self.shell.display_pub.publish = wrapped_publish
    #     try:
    #         yield captured
    #     finally:
    #         self.shell.display_pub.publish = orig_publish

    def get_cell_outputs_by_execution_count(self, notebook_path, code):
        """Retrieve outputs for the cell with the specified execution_count."""
        if not notebook_path or not os.path.exists(notebook_path):
            return [{'type': 'error', 'text': f"Notebook not found at {notebook_path}"}]
        
        try:
            with open(notebook_path, 'r', encoding='utf-8') as f:
                notebook = json.load(f)
            cells = notebook.get('cells', [])
            
            # Find cell with matching execution_count
            for cell in cells:
                if cell.get('cell_type') != 'code':
                    continue
                cell_code = cell.get('source')
                # print(''.join(cell_code).replace("\n", ""), code.replace("\n", ""))
                if ''.join(cell_code).replace("\n", "") == code.replace("\n", ""):
                    return cell.get('outputs', [])
                    outputs = cell.get('outputs', [])
                    if not outputs:
                        return [{'type': 'text', 'text': ""}]
                    
                    result = []
                    for output in outputs:
                        if output.get('output_type') == 'stream':
                            result.append({
                                'type': 'stream',
                                'name': output.get('name', 'stdout'),
                                'text': ''.join(output.get('text', []))
                            })
                        elif output.get('output_type') in ['execute_result', 'display_data']:
                            data = output.get('data', {})
                            if 'text/plain' in data:
                                result.append({
                                    'type': 'text',
                                    'text': ''.join(data['text/plain'])
                                })
                            elif 'image/png' in data:
                                result.append({
                                    'type': 'image',
                                    'text': 'Image output (PNG)'
                                })
                            else:
                                result.append({
                                    'type': 'text',
                                    'text': str(data)  # Fallback for other data types
                                })
                        elif output.get('output_type') == 'error':
                            result.append({
                                'type': 'error',
                                'text': f"{output.get('ename', '')}: {output.get('execution_count', '')}"
                            })
                    return result
            return [{'type': 'error', 'text': f""}]
        except Exception as e:
            return [{'type': 'error', 'text': f""}]
        
        
    async def do_execute(self, code, silent, store_history=True, user_expressions=None, allow_stdin=False, *
                        ,cell_meta=None, cell_id=None,):

        ip = self.shell
        
        #path to notebook directory
        notebook = self.shell.user_ns.get("__session__", None)
        notebook_path = "/".join(notebook.split("/")[:-1])
        
        
        # self.pre_run_cell(ip, code)
        # res = await super().do_execute(code, silent, store_history, user_expressions, allow_stdin)
        # analyze_algorithm()
        # self.post_run_cell(ip)
        # return res
    
        # check if notebook metadata says to audit it.
        if not self.nb_initialized:
            
            import_encodings = "import encodings.cp437\nimport encodings.iso8859_1\nimport encodings.utf_16\nimport codecs\ncodecs.lookup('cp437')\n"
            code = import_encodings + code
            
            def post_run_hook(_):
                with open(notebook, 'r', encoding='utf-8') as f:
                    # Capture system call for sciunit to move notebook in sandbox
                    f.read(1)
                
            # You can access result.info.raw_cell, result.result, etc.
            post_run_hook(None)
            ip.events.register("post_run_cell", post_run_hook)
            # from IPython.display import Javascript
            # from IPython.core.displaypub import publish_display_data
            # def enable_autosave_from_kernel():
            #     js = """
            #     if (typeof Jupyter !== "undefined" && Jupyter.notebook) {
            #         Jupyter.notebook.save_notebook();
            #     }
            #     """
            #     publish_display_data({'application/javascript': js}, {})
            # enable_autosave_from_kernel()
            # Load notebook
            nb = None
            with open(notebook, 'r', encoding='utf-8') as f:
                nb = nbformat.read(f, as_version=4)
                # set to audit mode
                # if nb.metadata['AUDIT'] == "true":
                #     self.audit = True

            # if self.audit:
            #     with open(notebook, 'w', encoding='utf-8') as f:
            #         nb.metadata['AUDIT'] = "false"
            #         nbformat.write(nb, f)                    
          
            self.nb_initialized = True

        # print(type(ip), type(ip.user_ns))
        self.pre_run_cell(ip, code)
        
        # # start_time = time.time()

        # # checkpoint_pattern = r"^# commit"
        # restore_pattern = r"^# rollback \[(\d+)\].*"

        # restore_match = re.match(restore_pattern, code.splitlines()[0])
        # # # checkpoint_match = re.match(checkpoint_pattern, code.splitlines()[0])
        
        # if restore_match:
        #     exec_id = re.findall(r'\b\d+\b', code.splitlines()[0])[0]
            
        #     loaded_vars = {}
        #     for f in range(int(exec_id ) + 1):
        #         self.run_checkout_command(f'{os.getcwd()}/session/vv', f, f'{os.getcwd()}/session/checkpoint_{f}.pkl')
        #         fname = f'{os.getcwd()}/session/checkpoint_{f}.pkl'
        #         if os.path.isfile(fname):
        #             with open(fname, 'rb') as dill_file:
        #                 loaded_vars.update(dill.load(dill_file))
        #             # os.remove(fname)
        
            
            
        #     # update global namespace with loaded variables
        #     for var in loaded_vars:
        #         # loaded_vars[var] = loaded_vars[var]['obj']
        #         self.shell.user_ns.update({var: loaded_vars[var]['obj']})

        #     # Build dependency graph
        #     deps_graph = {var: data['deps'] for var, data in loaded_vars.items() if data['obj'] is None}

        #     # Get execution order using topological sort
        #     execution_order = self.topological_sort(deps_graph)
        #     # print(loaded_vars)

        #     namespace = self.shell.user_ns

        #     # self.shell.user_ns['A'] = None

        #     # print(namespace['A'])
        #     # Execute code for each variable in order, only if missing
        #     for var_name in execution_order:
        #         # Check if the variable exists and is not None in the namespace
        #         # print(namespace['A'])
        #         if var_name in namespace and namespace[var_name] is not None:
        #             continue  # Skip if the object already exists and is not None
                
        #         # print(loaded_vars[var_name])
        #         code = str(loaded_vars[var_name]['code'])
        #         # print(type(str(code)))
        #         try:
        #             # Execute the code in the namespace
        #             # print("hello")
        #             exec(code, namespace)
        #             # If the variable isn't directly assigned, try to find it
        #             # if var_name not in namespace:
        #             #     for key, value in namespace.items():
        #             #         if hasattr(value, '__dict__') and var_name in value.__dict__:
        #             #             namespace[var_name] = value.__dict__[var_name]
        #             #             break
        #         except Exception as e:
        #             print(f"Error restoring {var_name}: {str(e)}")
        #             continue
        #         # print(f"Restored checkpoint {f}")                        
                        
        #         # else:
        #         #     continue
        #         #     print("No checkpoint found for the given ID")
        
            
        #     # ip = get_ipython()
        #     # res =  await super().do_execute(code, silent, store_history, user_expressions, allow_stdin)
            
        #     # accessed_vars = ip.tracked_ns.accessed
        # #     to_dump = {}
        # #     for v in accessed_vars:
        # #         to_dump[v] = self.shell.user_ns[v]

        # #     with open(f'{os.getcwd()}/session/checkpoint_{self.shell.execution_count - 1}.pkl', 'wb') as dill_file: 
        # #         # dill.dump_module(dill_file, self.shell)
        # #         # print(to_dump)
        # #         dill.dump(to_dump, dill_file)
            
        # #     self.post_run_cell(ip)
        # #     return res

        # # # if checkpoint_match:
        # # #     result =  await super().do_execute(code , silent, store_history, user_expressions, allow_stdin)
        # # #     self.dump_namespace()
        # #     # return result

        # # # if not self.initalized:
        # # #     self.initalized = True
        # # #     patch_namespace(self.shell)
        
        # if self.audit:
        if ("audit" in code or "flow mode normal" in code) and (code.strip().startswith('%') or code.strip().startswith('%%')):
            # If the code is a magic command, execute it directly
            import_encodings = "import encodings.cp437\nimport encodings.iso8859_1\nimport encodings.utf_16\nimport codecs\ncodecs.lookup('cp437')\n"
            code = import_encodings + code
            res = await super().do_execute(code, silent, store_history, user_expressions, allow_stdin)
            self.post_run_cell(ip)
            return res
        

        # c_id = MetadataHandler(notebook_path).get_checkpoint(code, self.last_cid)
        c_id = MetadataHandler(notebook_path).get_checkpoint(code, self.last_cid)
        # print(c_id)
        if self.audit:
            # print("auditing")            
            res = await super().do_execute(code, silent, store_history, user_expressions, allow_stdin)
            if not c_id:
                self.persist_metadata(notebook_path, code)
                self.dump1(ip)
                self.run_commit_command(f'{notebook_path}/vv', self.last_cid, f'{notebook_path}/checkpoint_{self.last_cid}.pkl')
                self.post_run_cell(ip)
            return res
        else:
            if c_id:
                print("restoring")
                self.load_memoized_cell(notebook_path, c_id)
                # if not silent:
                #     try:
                #         output_list = json.loads(outputs)
                #         for output in output_list:
                #             self.send_response(self.iopub_socket, output.get("output_type"), output)
                #     except Exception as e:
                #         # self.log.warning(f"Failed to replay outputs: {e}")
                #         pass

                outputs = self.get_cell_outputs_by_execution_count(notebook, code)
                def normalize_output_data(data):
                    normalized = {}
                    for mime, val in data.items():
                        if isinstance(val, list):
                            normalized[mime] = ''.join(val)
                        else:
                            normalized[mime] = val
                    return normalized
                
                for output in outputs:
                    output_type = output.get('output_type')

                    if output_type == 'stream':
                        self.send_response(self.iopub_socket, 'stream', {
                            'name': output.get('name', 'stdout'),
                            'text': ''.join(output.get('text', []))
                        })

                    elif output_type == 'execute_result':
                        self.send_response(self.iopub_socket, 'execute_result', {
                            'execution_count': self.shell.execution_count,
                            'data': normalize_output_data(output.get('data', {})),
                            'metadata': output.get('metadata', {})
                        })

                    elif output_type == 'display_data':
                        self.send_response(self.iopub_socket, 'display_data', {
                            'data': normalize_output_data(output.get('data', {})),
                            'metadata': output.get('metadata', {})
                        })

                    elif output_type == 'error':
                        self.send_response(self.iopub_socket, 'error', {
                            'ename': output.get('ename', ''),
                            'evalue': output.get('evalue', ''),
                            'traceback': output.get('traceback', [])
                        })
                
                # output_text = []
                # for output in outputs:
                #     output_text.append(output['text'])
                # output_text = '\n'.join(output_text) or ""
                
                # self.shell.execution_count += 1
                # self.send_response(self.iopub_socket, 'execute_result', {
                #     'execution_count': self.shell.execution_count,
                #     'data': {'text/plain': output_text},
                #     'metadata': {}
                # })

                self.post_run_cell(ip)
                self.last_cid = c_id
                self.shell.execution_count += 1
                return {
                    "status": "ok",
                    "execution_count": self.shell.execution_count,
                    "payload": [],
                    "user_expressions": {},
                }                

                # self.post_run_cell(ip)
                # self.last_cid = c_id
                # return {
                #     "status": "ok",
                #     "execution_count": self.shell.execution_count,
                #     "payload": [],
                #     "user_expressions": {},
                # }
            else:
                # "just executing"
                res = await super().do_execute(code, silent, store_history, user_expressions, allow_stdin)
                self.post_run_cell(ip)
                return res
    
        # c_id = MetadataHandler(notebook_path).get_checkpoint(code, self.last_cid)
        # if c_id and not self.audit:
        #     self.load_memoized_cell(notebook_path, c_id)
        #     outputs = self.get_cell_outputs_by_execution_count(notebook, self.shell.execution_count)
        #     output_text = []
        #     for output in outputs:
        #         output_text.append(output['text'])
        #     output_text = '\n'.join(output_text) or ""
            
        #     self.shell.execution_count += 1
        #     self.send_response(self.iopub_socket, 'execute_result', {
        #         'execution_count': self.shell.execution_count,
        #         'data': {'text/plain': output_text},
        #         'metadata': {}
        #     })

        #     self.post_run_cell(ip)
        #     self.last_cid = c_id
        #     return {
        #         "status": "ok",
        #         "execution_count": self.shell.execution_count,
        #         "payload": [],
        #         "user_expressions": {},
        #     }
        # else:
        #     res = await super().do_execute(code, silent, store_history, user_expressions, allow_stdin)
        #     if self.audit:
        #         if not c_id:
        #             self.persist_metadata(notebook_path, code)
        #             self.dump(ip)
        #             self.run_commit_command(f'{notebook_path}/vv', self.last_cid, f'{notebook_path}/checkpoint_{self.last_cid}.pkl')
        #     self.post_run_cell(ip)
        #     if not self.audit:
        #         self.last_cid = None # because the user ran a cell breaking the checkpoint graph order
        #     return res
            
        if not self.nb_initialized:
            self.shell.run_line_magic("flow", "mode normal")
            # exec("%flow mode normal", self.shell.user_ns)
            self.nb_initialized = True
                
        # # print(cell_meta)
        # if not self.diverged:
        #     # if code ran did change compared to persisted metadata set diverged to True
        #     # print(self.shell.execution_count)
        #     same = self.load_memoized_cell(notebook_path, code, self.shell.execution_count)
        #     if same:
        #         print("hello")
        #         # cell_meta["skip_execution"] = True
        #         # print(res["payload"])
        #         # res = await super().do_execute(code, silent, store_history, user_expressions, allow_stdin, cell_meta=cell_meta, cell_id=cell_id)
        #         # res = await super().do_execute(code, True, False, {}, allow_stdin, cell_meta=cell_meta, cell_id=cell_id)
        #         # print(res["payload"])
        #         # return
                
        #         outputs = self.get_cell_outputs_by_execution_count(notebook, self.shell.execution_count)
        #         output_text = []
        #         for output in outputs:
        #             # if output['type'] == 'error':
        #             #     output_text.append(f"Error: {output['text']}")
        #             # elif output['type'] == 'stream':
        #             #     output_text.append(f"{output['name']}: {output['text']}")
        #             # else:
        #             output_text.append(output['text'])
        #         output_text = '\n'.join(output_text) or ""
        #         # print(output_text)
                
        #         self.shell.execution_count += 1
        #         self.send_response(self.iopub_socket, 'execute_result', {
        #             'execution_count': self.execution_count - 1,
        #             'data': {'text/plain': output_text},
        #             'metadata': {}
        #         })

        #         # res["execution_count"] = self.shell.execution_count - 1
        #         # return res
        #         # self.shell.displayhook.disable()
        #         # self.shell.display_pub.publish = lambda *args, **kwargs: None
        #         # # return res
        #         # self.diverged = not same
        #         # self.shell.execution_count += 1
        #         # /Users/talhaazaz/Documents/DePaul/Research/NBRewind/CIKM_NB_v2.ipynb
                
                
        #         return {
        #             "status": "ok",
        #             "execution_count": self.shell.execution_count - 1,
        #             "payload": [],
        #             "user_expressions": {},
        #         }
        #     # from IPython.core.interactiveshell import DisplayHook
        #     # self.shell.displayhook = DisplayHook(self.shell)
        #     self.diverged = not same

        # print("executing")
        # res = await super().do_execute(code, silent, store_history, user_expressions, allow_stdin)
        
        # # if end_time - start_time > Threshold and res["status"] == "ok":
        # #     self.dump_namespace()
        # accessed_vars = ip.tracked_ns.accessed
        # # print(accessed_vars)
        # to_dump = {}
        # for v in accessed_vars:
        #     if v in self.shell.user_ns:
        #         to_dump[v] = self.shell.user_ns[v]
        # # with open(f'{os.getcwd()}/session/checkpoint_{self.shell.execution_count - 1}.pkl', 'wb') as dill_file: 
        # #     dill.dump(to_dump, dill_file)


        # res = await super().do_execute(code, silent, store_history, user_expressions, allow_stdin)
         
        
        # exclusion_list = ["print", "display", "fake_edge_sym", "ipyflow", "flow", "aliases", "_"]
        # to_persist = {}
        # # print(ip.tracked_ns.accessed)
        # for mem in flow().aliases:
        #     entrySet = flow().aliases[mem]
        #     varSet = []
        #     should_persist = False
        #     for entry in entrySet:
                
        #         if entry.readable_name.startswith("<literal_sym_"):
        #             continue
        #         if "__ipyflow_mutation" in entry.readable_name:
        #             continue
                
        #         var = entry.full_path[1]
        #         if var in exclusion_list:
        #             continue
               
        #         varSet.append(var)
        #         if var in ip.tracked_ns.accessed: 
        #             should_persist = True

            
        #     for var in varSet:
        #         if should_persist:
        #             to_persist[var] = self.shell.user_ns[var]
                
        # print(to_persist)
        # with open(f'{os.getcwd()}/session/checkpoint_{self.shell.execution_count - 1}.pkl', 'wb') as dill_file: 
        #     # dill.dump_module(dill_file, self.shell)
        #     dill.dump(to_persist, dill_file)
        
        # print("persisting metadata")
        
        # self.persist_metadata(notebook_path, code, self.shell.execution_count - 1)
        # self.dump(ip)
        # self.run_commit_command(f'{notebook_path}/vv', self.shell.execution_count - 1, f'{notebook_path}/session/checkpoint_{self.shell.execution_count - 1}.pkl')
        # self.post_run_cell(ip) 
        # end = time.time()

        # self.average_checkpoint_time = (self.average_checkpoint_time + (end - start)) / 2
        # self.checkpnt_times.append(end - start)
        # print(f"Checkpoint time: {end - start}")
        # print(f"Average checkpoint time: {self.average_checkpoint_time}")
        # self.times.append(end - start)
        # print("times: ", self.times)    
        # print("checkpnt_times: ", self.checkpnt_times)

        # return res


if __name__ == '__main__':
    
    IPKernelApp.launch_instance(kernel_class=CustomKernel)
