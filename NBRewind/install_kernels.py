import os
import json
from pathlib import Path

def update_and_install_kernels():
    # Get the absolute path to the project root
    project_root = Path(__file__).parent.absolute()
    # Get the absolute path to nbrewind_flinc.py
    nbrewind_path = str(project_root / "nbrewind_flinc.py")    
    # Get the absolute path to handler.py
    handler_path = str(project_root / "kernels" / "audit-kernel" / "handler.py")
    # updating audit-kernel
    kernel_json_path = project_root / "kernels" / "audit-kernel" / "kernel.json"
    # Read the existing kernel.json
    with open(kernel_json_path, 'r') as f:
        kernel_json = json.load(f)    
    # Update the paths
    kernel_json["argv"][1] = handler_path
    kernel_json["argv"][5] = nbrewind_path

    with open(kernel_json_path, 'w') as f:
        json.dump(kernel_json, f, indent=4)
    # Remove existing audit-kernel if it exists
    os.system("jupyter kernelspec remove audit-kernel -f")
    # Install audit-kernel
    os.system(f"jupyter kernelspec install {kernel_json_path.parent} --user")
    print("Installed audit-kernel")

    # updating nbrewind-kernel
    kernel_json_path = project_root / "kernels" / "nbrewind-kernel" / "kernel.json"
    # Read the existing kernel.json
    with open(kernel_json_path, 'r') as f:
        kernel_json = json.load(f)    
    # Update the paths
    kernel_json["argv"][1] = nbrewind_path

    with open(kernel_json_path, 'w') as f:
        json.dump(kernel_json, f, indent=4)

    # Remove existing nbrewind-kernel if it exists
    os.system("jupyter kernelspec remove nbrewind-kernel -f")
    # Install nbrewind-kernel
    os.system(f"jupyter kernelspec install {kernel_json_path.parent} --user")
    print("Installed nbrewind-kernel")

if __name__ == "__main__":
    update_and_install_kernels()