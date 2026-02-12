import subprocess


def pytest_configure(config):
    """Clean up any stale test containers and networks from previous runs at session start."""
    if hasattr(config, 'workerinput'):
        # Only run on the controller, not on xdist workers
        return

    for prefix in ["orthanc_test_", "azurite_test_"]:
        result = subprocess.run(
            ["docker", "ps", "-a", "--filter", f"name={prefix}", "--format", "{{.Names}}"],
            capture_output=True, text=True
        )
        for name in result.stdout.strip().split('\n'):
            if name:
                subprocess.run(["docker", "stop", name], capture_output=True)
                subprocess.run(["docker", "rm", name], capture_output=True)

    # Clean up stale Docker networks from previous test runs
    result = subprocess.run(
        ["docker", "network", "ls", "--filter", "name=orthanc_net_", "--format", "{{.Name}}"],
        capture_output=True, text=True
    )
    for name in result.stdout.strip().split('\n'):
        if name:
            subprocess.run(["docker", "network", "rm", name], capture_output=True)
