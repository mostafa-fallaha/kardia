import subprocess
from dotenv import load_dotenv
import os

load_dotenv()
version = os.getenv('VERSION')

def increment_version(version):
    major, minor = map(int, version.split('.'))
    if minor == 9:
        major += 1
        minor = 0
    else:
        minor += 1
    return f'{major}.{minor}'

new_version = increment_version(version)

tag_version = f'v{new_version}'
commit_message = f'Push data version v{new_version}'

data_folder = f'ETL/dvc_data/data/'
dvc_file = f'ETL/dvc_data/data.dvc'
gitignore_file = 'ETL/dvc_data/.gitignore'

# print(f'tag: {tag_version} - commit: {commit_message}')

subprocess.run(["dvc", "add", data_folder], check=True)
subprocess.run(["git", "add", dvc_file, gitignore_file], check=True)
subprocess.run(["git", "commit", "-m", commit_message], check=True)
subprocess.run(["git", "tag", "-a", tag_version, "-m", f'version v{new_version}'], check=True)
subprocess.run(["dvc", "push"], check=True)
subprocess.run(["git", "push"], check=True)
subprocess.run(["git", "push", "origin", "--tags"], check=True)

#==============================================================================================
#====================== Increase the version by1 ==============================================

with open('.env', 'r') as file:
    lines = file.readlines()
with open('.env', 'w') as file:
    for line in lines:
        if line.startswith('VERSION'):
            file.write(f'VERSION={new_version}\n')
        else:
            file.write(line)

print("--- Data versioned successfully ---")