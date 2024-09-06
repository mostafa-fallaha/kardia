try {
    python version_data.py
} catch {
    Write-Host "An error occurred: $($_.Exception.Message)"
    exit 1
}


