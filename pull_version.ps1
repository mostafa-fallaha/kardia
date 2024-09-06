param (
    [string]$Tag
)

if (-not $Tag) {
    Write-Host "Usage: .\pull_version.ps1 <tag>"
    exit 1
}

# List of possible branch names
$branches = @("main", "master") # you can add more here
$mainBranch

foreach ($branch in $branches) {
    $branchExists = git rev-parse --verify --quiet $branch
    if ($branchExists) {
        $mainBranch = $branch
    }
}

try {
    # Run the specified Python script with the commit message
    python pull_by_version.py $Tag $mainBranch
} catch {
    Write-Host "An error occurred: $($_.Exception.Message)"
    exit 1
}
