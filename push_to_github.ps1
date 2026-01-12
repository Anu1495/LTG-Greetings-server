param(
    [string]$DefaultRepoName = "LTG-Greetings-server",
    [string]$Visibility = "public"
)

if (-not (Get-Command gh -ErrorAction SilentlyContinue)) {
    Write-Host "GitHub CLI 'gh' not found. Install from https://cli.github.com/ and re-run." -ForegroundColor Yellow
    exit 1
}

$repoName = Read-Host "Repo name (enter to accept default)" -Prompt "Repo name"
if ([string]::IsNullOrWhiteSpace($repoName)) { $repoName = $DefaultRepoName }

Write-Host "Creating GitHub repo $repoName and pushing..."

if (-not (Test-Path .git)) {
    git init
}
git add .
git rev-parse --verify HEAD 2>$null
if ($LASTEXITCODE -ne 0) {
    git commit -m "Initial commit" 2>$null
} else {
    git commit -m "Update" 2>$null
    if ($LASTEXITCODE -ne 0) {
        Write-Host "No changes to commit"
    }
}

gh repo create $repoName --$Visibility --source=. --remote=origin --push

Write-Host "Done. Remote origin set to:" -ForegroundColor Green
git remote -v
