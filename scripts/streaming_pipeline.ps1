#!/usr/bin/env pwsh
Write-Host "Starting Real-Time Fashion Pipeline..." -ForegroundColor Green
Write-Host "Running inside Docker container for full automation" -ForegroundColor Cyan

# Set working directory
Set-Location /workspace

while ($true) {
    try {
        Write-Host "$(Get-Date): Running Bronze → Silver..." -ForegroundColor Yellow
        
        # Use docker-compose from within container
        $bronzeProcess = Start-Process -FilePath "docker-compose" -ArgumentList "run", "--rm", "all-sources-batch" -Wait -PassThru -NoNewWindow
        $bronzeExitCode = $bronzeProcess.ExitCode
        
        if ($bronzeExitCode -eq 0) {
            Write-Host "$(Get-Date): Bronze → Silver completed, starting Silver → Gold..." -ForegroundColor Yellow
            
            $goldProcess = Start-Process -FilePath "docker-compose" -ArgumentList "run", "--rm", "silver-to-gold" -Wait -PassThru -NoNewWindow
            $goldExitCode = $goldProcess.ExitCode
            
            if ($goldExitCode -eq 0) {
                Write-Host "$(Get-Date): Pipeline cycle completed successfully! 🎉" -ForegroundColor Green
            } else {
                Write-Host "$(Get-Date): Silver → Gold failed! ❌" -ForegroundColor Red
            }
        } else {
            Write-Host "$(Get-Date): Bronze → Silver failed! ❌" -ForegroundColor Red
        }
        
        Write-Host "$(Get-Date): Sleeping for 60 seconds..." -ForegroundColor Cyan
        Start-Sleep -Seconds 60
        
    } catch {
        Write-Host "$(Get-Date): Pipeline error: $($_.Exception.Message)" -ForegroundColor Red
        Write-Host "$(Get-Date): Retrying in 60 seconds..." -ForegroundColor Yellow
        Start-Sleep -Seconds 60
    }
}