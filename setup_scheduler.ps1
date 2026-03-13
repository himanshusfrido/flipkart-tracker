# ============================================================
# Flipkart Tracker - Windows Task Scheduler Setup
# Run this script as Administrator in PowerShell
# ============================================================

Write-Host "========================================" -ForegroundColor Cyan
Write-Host "  Flipkart Tracker - Task Scheduler Setup" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan
Write-Host ""

# Check for admin privileges
$isAdmin = ([Security.Principal.WindowsPrincipal] [Security.Principal.WindowsIdentity]::GetCurrent()).IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)
if (-not $isAdmin) {
    Write-Host "WARNING: Not running as Administrator. Tasks may not have 'run whether logged on or not' capability." -ForegroundColor Yellow
    Write-Host "To set that option, re-run this script as Administrator." -ForegroundColor Yellow
    Write-Host ""
}

# Configuration
$nodePath = (Get-Command node -ErrorAction SilentlyContinue).Source
if (-not $nodePath) {
    $nodePath = "C:\Program Files\nodejs\node.exe"
    if (-not (Test-Path $nodePath)) {
        Write-Host "ERROR: Node.js not found. Please install Node.js or update the path." -ForegroundColor Red
        exit 1
    }
}
Write-Host "Node.js path: $nodePath" -ForegroundColor Green

$scriptPath = "C:\Users\HimanshuSingh\flipkart-tracker\scraper_full.js"
if (-not (Test-Path $scriptPath)) {
    Write-Host "ERROR: Scraper script not found at $scriptPath" -ForegroundColor Red
    exit 1
}
Write-Host "Scraper script: $scriptPath" -ForegroundColor Green

$workingDir = "C:\Users\HimanshuSingh\flipkart-tracker"
$userName = $env:USERNAME
$domainUser = "$env:USERDOMAIN\$env:USERNAME"

Write-Host "Working directory: $workingDir" -ForegroundColor Green
Write-Host "User: $domainUser" -ForegroundColor Green
Write-Host ""

# Define the three scheduled tasks
$tasks = @(
    @{
        Name = "Flipkart Tracker 1030AM"
        Hour = 10
        Minute = 30
        Description = "Flipkart product tracker - Morning run at 10:30 AM"
    },
    @{
        Name = "Flipkart Tracker 0430PM"
        Hour = 16
        Minute = 30
        Description = "Flipkart product tracker - Afternoon run at 4:30 PM"
    },
    @{
        Name = "Flipkart Tracker 1030PM"
        Hour = 22
        Minute = 30
        Description = "Flipkart product tracker - Night run at 10:30 PM"
    }
)

foreach ($task in $tasks) {
    Write-Host "-------------------------------------------" -ForegroundColor DarkGray
    Write-Host "Creating task: $($task.Name)" -ForegroundColor Yellow

    # Remove existing task if it exists
    $existing = Get-ScheduledTask -TaskName $task.Name -ErrorAction SilentlyContinue
    if ($existing) {
        Write-Host "  Removing existing task..." -ForegroundColor DarkYellow
        Unregister-ScheduledTask -TaskName $task.Name -Confirm:$false
    }

    # Create the action: run node with the scraper script
    $action = New-ScheduledTaskAction `
        -Execute $nodePath `
        -Argument "`"$scriptPath`"" `
        -WorkingDirectory $workingDir

    # Create the trigger: daily at specified time
    $trigger = New-ScheduledTaskTrigger `
        -Daily `
        -At ([DateTime]::Today.AddHours($task.Hour).AddMinutes($task.Minute))

    # Task settings
    $settings = New-ScheduledTaskSettingsSet `
        -AllowStartIfOnBatteries `
        -DontStopIfGoingOnBatteries `
        -StartWhenAvailable `
        -ExecutionTimeLimit (New-TimeSpan -Hours 4) `
        -RestartCount 2 `
        -RestartInterval (New-TimeSpan -Minutes 5) `
        -MultipleInstances IgnoreNew

    # Register the task
    try {
        if ($isAdmin) {
            # Register to run whether user is logged on or not
            $principal = New-ScheduledTaskPrincipal -UserId $domainUser -LogonType S4U -RunLevel Limited
            Register-ScheduledTask `
                -TaskName $task.Name `
                -Action $action `
                -Trigger $trigger `
                -Settings $settings `
                -Principal $principal `
                -Description $task.Description `
                -Force | Out-Null
            Write-Host "  Created (runs whether logged on or not)" -ForegroundColor Green
        } else {
            # Register for current user only (no password required)
            Register-ScheduledTask `
                -TaskName $task.Name `
                -Action $action `
                -Trigger $trigger `
                -Settings $settings `
                -Description $task.Description `
                -Force | Out-Null
            Write-Host "  Created (runs only when logged on)" -ForegroundColor Green
        }
    } catch {
        Write-Host "  FAILED to create task: $_" -ForegroundColor Red
    }
}

Write-Host ""
Write-Host "========================================" -ForegroundColor Cyan
Write-Host "  Setup Complete!" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan
Write-Host ""

# Show all tasks
Write-Host "Registered tasks:" -ForegroundColor Yellow
foreach ($task in $tasks) {
    $t = Get-ScheduledTask -TaskName $task.Name -ErrorAction SilentlyContinue
    if ($t) {
        $info = $t | Get-ScheduledTaskInfo -ErrorAction SilentlyContinue
        $triggerTime = $t.Triggers[0].StartBoundary
        Write-Host "  [OK] $($task.Name) - Daily at $triggerTime" -ForegroundColor Green
    } else {
        Write-Host "  [MISSING] $($task.Name)" -ForegroundColor Red
    }
}

Write-Host ""
Write-Host "To test manually, run:" -ForegroundColor Cyan
Write-Host "  node `"$scriptPath`" --dry-run" -ForegroundColor White
Write-Host ""
Write-Host "To view tasks in Task Scheduler:" -ForegroundColor Cyan
Write-Host "  taskschd.msc" -ForegroundColor White
Write-Host ""
