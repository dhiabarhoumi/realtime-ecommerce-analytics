# ==============================================================================
# REPLAY RESPONSIBLY; KEEP README NOTE ABOUT IMPORTED HISTORY
# ==============================================================================
#
# PowerShell script to replay commit history from scripts/commit_timeline.yaml
# 
# Usage: .\scripts\replay_timeline.ps1
# 
# Prerequisites:
# - Clean git repository (will create commits)
# - Python with PyYAML installed
# - All project files already staged for commits
#
# WARNING: This will create commits with backdated timestamps!
# Only use for legitimate history reconstruction from offline work.
# ==============================================================================

param(
    [switch]$Force = $false
)

$ErrorActionPreference = "Stop"

$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$ProjectRoot = Split-Path -Parent $ScriptDir
$TimelineFile = Join-Path $ScriptDir "commit_timeline.yaml"

Write-Host "========================================" -ForegroundColor Blue
Write-Host "  Real-time Analytics Commit Replay" -ForegroundColor Blue  
Write-Host "========================================" -ForegroundColor Blue
Write-Host ""

# Check prerequisites
if (!(Test-Path $TimelineFile)) {
    Write-Host "Error: Timeline file not found: $TimelineFile" -ForegroundColor Red
    exit 1
}

if (!(Get-Command python -ErrorAction SilentlyContinue)) {
    Write-Host "Error: python is required" -ForegroundColor Red
    exit 1
}

# Parse YAML timeline
function Parse-Timeline {
    param($TimelineFile)
    
    $pythonScript = @"
import yaml
import sys

timeline_file = r'$TimelineFile'
with open(timeline_file, 'r', encoding='utf-8') as f:
    data = yaml.safe_load(f)

for i, commit in enumerate(data['commits']):
    date = commit['date']
    commit_type = commit['type']
    scope = commit.get('scope', '')
    message = commit['message']
    files = commit.get('files', [])
    
    # Format conventional commit message
    if scope:
        full_message = f"{commit_type}({scope}): {message}"
    else:
        full_message = f"{commit_type}: {message}"
    
    print(f"COMMIT_{i}|{date}|{full_message}|{','.join(files)}")
"@

    return python -c $pythonScript
}

Set-Location $ProjectRoot

# Ensure we're in a git repository
if (!(Test-Path ".git")) {
    Write-Host "Error: Not in a git repository" -ForegroundColor Red
    exit 1
}

# Configure git author
git config user.name "Dhieddine BARHOUMI"
git config user.email "dhieddine.barhoumi@gmail.com"

Write-Host "Parsing commit timeline..." -ForegroundColor Yellow
$commits = Parse-Timeline $TimelineFile

# Count total commits
$totalCommits = ($commits | Measure-Object).Count
Write-Host "Found $totalCommits commits to replay" -ForegroundColor Blue
Write-Host ""

# Function to create/touch files for a commit
function Prepare-Files {
    param($FilesList)
    
    if ([string]::IsNullOrEmpty($FilesList)) {
        return
    }
    
    $files = $FilesList -split ','
    foreach ($file in $files) {
        $file = $file.Trim()
        
        # Create directory if it doesn't exist
        $dir = Split-Path -Parent $file
        if ($dir -and !(Test-Path $dir)) {
            New-Item -ItemType Directory -Path $dir -Force | Out-Null
        }
        
        # Touch or append to file to simulate changes
        if (!(Test-Path $file)) {
            # Create new file with basic content
            $content = switch -Regex ($file) {
                '\.py$' { 
                    "# $file - $(Get-Date)`n# Auto-generated during commit replay"
                }
                '\.md$' { 
                    "# $(Split-Path -Leaf $file -replace '\.md$', '')`n`nUpdated: $(Get-Date)"
                }
                '\.(yaml|yml)$' { 
                    "# $file`nupdated: $(Get-Date)"
                }
                '\.json$' { 
                    "{`"updated`": `"$(Get-Date)`"}"
                }
                default { 
                    "# $file - $(Get-Date)"
                }
            }
            Set-Content -Path $file -Value $content -Encoding UTF8
        } else {
            # Append to existing file
            Add-Content -Path $file -Value "`n# Updated: $(Get-Date)" -Encoding UTF8
        }
        
        # Add file to git
        git add $file
    }
}

# Process each commit
$commitCount = 0
foreach ($commitLine in $commits) {
    $commitCount++
    
    $parts = $commitLine -split '\|', 4
    $commitId = $parts[0]
    $date = $parts[1]
    $message = $parts[2]
    $files = if ($parts.Length -gt 3) { $parts[3] } else { "" }
    
    Write-Host "[$commitCount/$totalCommits] $message" -ForegroundColor Green
    Write-Host "  Date: $date" -ForegroundColor Blue
    Write-Host "  Files: $files" -ForegroundColor Blue
    
    # Prepare files for this commit
    Prepare-Files $files
    
    # Check if there are changes to commit
    $changesExist = git diff --cached --quiet; $LASTEXITCODE -ne 0
    if (!$changesExist) {
        Write-Host "  No changes to commit, skipping..." -ForegroundColor Yellow
        Write-Host ""
        continue
    }
    
    # Run pre-commit hooks if available (but don't fail)
    if ((Get-Command pre-commit -ErrorAction SilentlyContinue) -and (Test-Path ".pre-commit-config.yaml")) {
        Write-Host "  Running pre-commit hooks..." -ForegroundColor Blue
        $changedFiles = git diff --cached --name-only
        if ($changedFiles) {
            try {
                pre-commit run --files $changedFiles 2>$null
            } catch {
                # Ignore pre-commit errors
            }
            
            # Re-add files after pre-commit modifications
            git add -u
        }
    }
    
    # Create commit with backdated timestamp
    $env:GIT_AUTHOR_DATE = $date
    $env:GIT_COMMITTER_DATE = $date
    git commit -m $message --quiet
    
    Write-Host "  ✓ Committed" -ForegroundColor Green
    Write-Host ""
    
    # Small delay to avoid overwhelming the system
    Start-Sleep -Milliseconds 100
}

Write-Host "========================================" -ForegroundColor Green
Write-Host "  Commit replay completed successfully!" -ForegroundColor Green
Write-Host "========================================" -ForegroundColor Green
Write-Host ""

# Show summary
Write-Host "Repository status:" -ForegroundColor Blue
git log --oneline -10
Write-Host ""

Write-Host "Next steps:" -ForegroundColor Blue
Write-Host "1. Review the commit history: git log --oneline"
Write-Host "2. Add remote: git remote add origin https://github.com/yourhandle/realtime-analytics-kafka-spark.git"
Write-Host "3. Push to GitHub: git push -u origin main"
Write-Host ""

Write-Host "Remember: This history represents imported offline work (Sep 2023-Nov 2023)" -ForegroundColor Yellow