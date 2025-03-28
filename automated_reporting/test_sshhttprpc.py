import subprocess

def ssh_exec(host, command):
  try:
    # Form the SSH command
    ssh_command = f"ssh {host} {command}"

    # Use subprocess to execute the command
    process = subprocess.Popen(
      ssh_command,
      shell=True,  # Allow the shell to interpret the command
      stdout=subprocess.PIPE,  # Capture standard output
      stderr=subprocess.PIPE,  # Capture standard error
      text=True,  # Return output as a string
    )

    # Wait for the command to complete
    stdout, stderr = process.communicate()

    # Return output and errors
    return stdout.strip(), stderr.strip()
  except Exception as e:
    return None, str(e)

# Example usage
if __name__ == "__main__":
  host = "c1"
  command = "curl -sSX GET 'http://0.0.0.0:51061/v1/pnp/self-test/29861f33-c48e-4678-b761-8f103222bf87'"

  stdout, stderr = execute_ssh_command(host, command)
  
  if stdout:
    print("Command Output:")
    print(stdout)
  if stderr:
    print("Command Error:")
    print(stderr)
