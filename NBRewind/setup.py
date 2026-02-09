# from setuptools import setup, find_packages

# setup(
#     name='nbrewind',
#     version='0.1',
#     packages=find_packages(),
#     install_requires=[],
#     entry_points={
#         'console_scripts': [
#             'nbrewind=nbrewind.cli:main',
#         ],
#     },
#     author='Your Name',
#     description='A CLI tool to audit and repeat Jupyter notebooks',
#     classifiers=[
#         'Programming Language :: Python :: 3',
#         'Environment :: Console',
#         'Operating System :: OS Independent',
#     ],
#     # python_requires='<=3.1',
# )

from setuptools import setup, find_packages
from setuptools.command.install import install
import subprocess
import os

class InstallWithKernels(install):
    
    def patch_sciunit(self):
        import site
        import pathlib

        def replace_line(file_path, match_line, new_line):
            lines = file_path.read_text().splitlines()
            updated_lines = []
            for line in lines:
                if line.strip().startswith(match_line):
                    updated_lines.append(new_line)
                else:
                    updated_lines.append(line)
            file_path.write_text('\n'.join(updated_lines) + '\n')

        def insert_before_class(file_path, insert_lines):
            lines = file_path.read_text().splitlines()
            for i, line in enumerate(lines):
                if line.strip().startswith("class "):
                    lines = lines[:i] + insert_lines + lines[i:]
                    break
            file_path.write_text('\n'.join(lines) + '\n')

        for sp in site.getsitepackages():
            sciunit_root = pathlib.Path(sp) / "sciunit2"
            if not sciunit_root.exists():
                continue

            # Patch 1: signal handler in sciunit2/command/exec_/__init__.py
            exec_init = sciunit_root / "command" / "exec_" / "__init__.py"
            if exec_init.exists():
                insert_before_class(exec_init, [
                    "import signal",
                    "signal.signal(signal.SIGINT, lambda *_: None)"
                ])

            # Patch 2: unpack fix in sciunit2/core.py
            core_file = sciunit_root / "core.py"
            if core_file.exists():
                new_code = core_file.read_text().replace("cd, ls = f", "cd, ls, *extra = f")
                core_file.write_text(new_code)
                # replace_line(core_file, "cd, ls = f", "cd, ls, *extra = f")

            # Patch 3: remove distutils.clear() in sciunit2/command/given.py
            given_file = sciunit_root / "command" / "given.py"
            if given_file.exists():
                new_code = given_file.read_text().replace("distutils.dir_util._path_created.clear()", "")
                given_file.write_text(new_code)
                # replace_line(given_file, "distutils.dir_util._path_created.clear()", "")

            print("✅ sciunit2 patched successfully.")
            break

    def run(self):
        # First run the normal install
        install.run(self)
        
        # Then install the kernels
        kernel_script = os.path.join(os.path.dirname(__file__), 'install_kernels.py')
        subprocess.run(['python', kernel_script], check=True)
        # print("patch sciunit2")
        # self.patch_sciunit()

setup(
    name='nbrewind',
    version='0.1',
    packages=find_packages(),
    install_requires=[
        'ipykernel',
        'ipyflow',
        'dill',
        'nbformat',
        'jupyter_client',
        # 'sciunit2'
    ],
    entry_points={
        'console_scripts': [
            'nbrewind=nbrewind.cli:main',
        ],
    },
    cmdclass={
        'install': InstallWithKernels,
    },
    author='Your Name',
    description='A CLI tool to audit and repeat Jupyter notebooks',
    classifiers=[
        'Programming Language :: Python :: 3',
        'Environment :: Console',
        'Operating System :: OS Independent',
    ],
)