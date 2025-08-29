<# 
    DmgSummary_grp.ps1
    ------------------------------------------------------------
    PURPOSE
      - Parse a Project Gorgon player log and summarize damage.
      - Preserve your existing behavior (aliases, health/armor split,
        fight counting, totals, ranking) AND add zone-based grouping.

    NEW FEATURES
      1) Zones are detected from lines like:  "Sent C_INIT2 for AreaPovus"
         • Each time this appears, a NEW "zone session" starts.
         • If you go Povus → Casino → Povus again, you'll get THREE
           separate zone sections in that SAME order.
      2) "Every Zone" overall summary at the end (all sessions combined).
      3) Safe defaults, plus optional CSV export switches.

    USAGE
      - Run as-is to parse the default path.
      - Or: .\DmgSummary_grp.ps1 -LogPath "C:\path\to\Player.log"
      - Optional: add -ExportCsv to write a CSV with all per-zone rows.
        You can control the path with -CsvPath "somefile.csv"

    NOTES
      - Regex for damage lines matches your original format:
           "<PlayerName>: <N> health dmg <M> armor dmg"
      - Aliases (player grouping) are preserved from your sample.
      - Zone order is **exactly** the order loaded in the log file.
#>

[CmdletBinding()]
param(
    # Path to the Project Gorgon player log
    [string]$LogPath = "C:\Users\USER\AppData\LocalLow\Elder Game\Project Gorgon\Player.log",

    # Export a flattened CSV of all per-zone, per-player rows
    [switch]$ExportCsv,

    # Where to write the CSV (only used if -ExportCsv is passed)
    [string]$CsvPath = ".\DamageSummary_ByZone.csv"
)

# --- Safety checks & input read ------------------------------------------------
if (-not (Test-Path -LiteralPath $LogPath)) {
    Write-Error "Log file not found: $LogPath"
    return
}

# Read the log file content
# Using -Raw would give a single string; we want to stream line-by-line for order.
$logContent = Get-Content -LiteralPath $LogPath

# --- Initialize structures -----------------------------------------------------
# Overall damage across ALL zones
$overallDamage = @{}       # key: MasterPlayer -> { HealthDamage, ArmorDamage, TotalDamage (added later) }
$fightCount    = 0         # Count fights (as in your original script)
$totalLines    = 0         # Just for optional diagnostics

# Zone session tracking (order matters!)
# Each time we see "Sent C_INIT2 for <ZoneName>" we start a new session.
$zoneSessions = New-Object System.Collections.ArrayList   # holds objects { SessionId, Name, Visit }
$currentSessionId = 0
$currentZoneName  = $null

# Per-zone, per-player damage:
# Dictionary: SessionId -> (Hashtable: MasterPlayer -> { HealthDamage, ArmorDamage, TotalDamage (added later) })
$zoneDamage = @{}

# Track how many times we've seen each zone name, so we can print "Visit #k"
$zoneVisitCount = @{}      # key: ZoneName -> Int

# --- Define player grouping (aliases) -----------------------------------------
# Keep your original mapping and structure.
$playerGroups = @{
    "Uber Poker"               = "Yder"
    "Yder Poison Bee"          = "Yder"
    "Sandstorm"                = "Yder"
    "Addled Figment"           = "Azizah"
    "Lyramis"                  = "Lyramr"
    "Summoned Golem Minion"    = "Azizah"
}

function Resolve-PlayerName {
    param([string]$name)
    if ($playerGroups.ContainsKey($name)) {
        return $playerGroups[$name]
    } else {
        return $name
    }
}

# Helper: start a new zone session (in order of appearance)
function Start-NewZoneSession {
    param([string]$zoneName)

    # Bump visit counter and remember this visit number
    if (-not $zoneVisitCount.ContainsKey($zoneName)) {
        $zoneVisitCount[$zoneName] = 0
    }
    $zoneVisitCount[$zoneName]++

    $script:currentSessionId++
    $session = [PSCustomObject]@{
        SessionId = $script:currentSessionId
        Name      = $zoneName
        Visit     = $zoneVisitCount[$zoneName]  # 1-based visit number for this zone name
    }
    [void]$zoneSessions.Add($session)

    # Initialize the per-player hashtable for this session
    $zoneDamage[$script:currentSessionId] = @{}

    # Update current name for quick reference
    $script:currentZoneName = $zoneName
}

# If the log has damage BEFORE any zone load line (unlikely, but safe),
# start an initial "Unknown" session so we don't drop that damage.
Start-NewZoneSession -zoneName "Unknown"

# --- Parse the log ------------------------------------------------------------
# Regex for zone changes. We capture *everything* after "Sent C_INIT2 for ".
# Example from your sample: "Sent C_INIT2 for AreaPovus"
$zoneChangeRegex = [regex]"Sent C_INIT2 for\s+(?<zone>.+)$"

# Regex for damage lines (same as your script)
# e.g., "Uber Poker: 123 health dmg 456 armor dmg"
$damageRegex = [regex]"(?<player>\w[\w\s]*):\s+(?<health>\d+)\s+health dmg\s+(?<armor>\d+)\s+armor dmg"

foreach ($line in $logContent) {
    $totalLines++

    # Fight boundary tracking (unchanged from your original)
    if ($line -match "Detailed Analysis:") {
        $fightCount++
        continue
    }

    # Detect zone changes in order and start a NEW zone session
    $zmatch = $zoneChangeRegex.Match($line)
    if ($zmatch.Success) {
        $zoneName = $zmatch.Groups['zone'].Value.Trim()
        Start-NewZoneSession -zoneName $zoneName
        continue
    }

    # Parse damage lines and attribute to the current zone session
    $dmatch = $damageRegex.Match($line)
    if ($dmatch.Success) {
        $rawPlayer    = $dmatch.Groups['player'].Value.Trim()
        $player       = Resolve-PlayerName $rawPlayer
        $healthDamage = [int]$dmatch.Groups['health'].Value
        $armorDamage  = [int]$dmatch.Groups['armor'].Value

        # --- Update per-zone stats ------------------------------------------
        if (-not $zoneDamage[$currentSessionId].ContainsKey($player)) {
            $zoneDamage[$currentSessionId][$player] = @{
                HealthDamage = 0
                ArmorDamage  = 0
            }
        }
        $zoneDamage[$currentSessionId][$player].HealthDamage += $healthDamage
        $zoneDamage[$currentSessionId][$player].ArmorDamage  += $armorDamage

        # --- Update overall stats (Every Zone) -------------------------------
        if (-not $overallDamage.ContainsKey($player)) {
            $overallDamage[$player] = @{
                HealthDamage = 0
                ArmorDamage  = 0
            }
        }
        $overallDamage[$player].HealthDamage += $healthDamage
        $overallDamage[$player].ArmorDamage  += $armorDamage

        continue
    }
}

# --- Compute totals (overall) -------------------------------------------------
$overallTotalDamage = 0
foreach ($player in $overallDamage.Keys) {
    $totalDamage = $overallDamage[$player].HealthDamage + $overallDamage[$player].ArmorDamage
    $overallDamage[$player] | Add-Member -MemberType NoteProperty -Name "TotalDamage" -Value $totalDamage
    $overallTotalDamage += $totalDamage
}

# --- Report: Header -----------------------------------------------------------
Write-Host ""
Write-Host "### Damage Report (Per Zone In Load Order) — for $fightCount fights"
Write-Host "Log path: $LogPath"
Write-Host "Lines parsed: $totalLines"
Write-Host ""

# --- Helper for table printing ------------------------------------------------
function Print-DamageTable {
    param(
        [Parameter(Mandatory=$true)] [System.Collections.IDictionary]$PlayerMap,
        [string]$Title,
        [int]$RankStart = 1
    )

    # Compute zone (or scope) total to provide percentages
    $scopeTotal = 0
    foreach ($p in $PlayerMap.Keys) {
        $scopeTotal += ($PlayerMap[$p].HealthDamage + $PlayerMap[$p].ArmorDamage)
    }

    if ($Title) {
        Write-Host $Title
    }

    # Print header
    Write-Host ("| {0,-4} | {1,-24} | {2,-14} | {3,-10} | {4,-14} | {5,-12} |" -f "Rank","Player","Total Damage","% of Total","Health Damage","Armor Damage")
    Write-Host ("| {0,-4} | {1,-24} | {2,-14} | {3,-10} | {4,-14} | {5,-12} |" -f "----","------------------------","--------------","----------","--------------","------------")

    # Sort players by total damage desc
    $sortedPlayers = $PlayerMap.Keys | Sort-Object { $PlayerMap[$_].HealthDamage + $PlayerMap[$_].ArmorDamage } -Descending

    $rank = $RankStart
    foreach ($player in $sortedPlayers) {
        $health = [int]$PlayerMap[$player].HealthDamage
        $armor  = [int]$PlayerMap[$player].ArmorDamage
        $total  = $health + $armor

        $pct = if ($scopeTotal -gt 0) { [math]::Round(($total / $scopeTotal) * 100, 2) } else { 0 }
        $pctFmt     = "{0:F2}%" -f $pct
        $healthFmt  = "{0:N0}" -f $health
        $armorFmt   = "{0:N0}" -f $armor
        $totalFmt   = "{0:N0}" -f $total

        Write-Host ("| {0,-4} | {1,-24} | {2,14} | {3,10} | {4,14} | {5,12} |" -f $rank, $player, $totalFmt, $pctFmt, $healthFmt, $armorFmt)
        $rank++
    }

    $scopeTotalFmt = "{0:N0}" -f $scopeTotal
    Write-Host ""
    Write-Host ("**Zone/Scope Total Damage: {0}**" -f $scopeTotalFmt)
    Write-Host ""
}

# --- Report: Per-Zone sections (in the exact order loaded) --------------------
$csvRows = New-Object System.Collections.Generic.List[object]

foreach ($session in $zoneSessions) {
    $sid  = $session.SessionId
    $name = $session.Name
    $visit= $session.Visit

    if ($zoneDamage.ContainsKey($sid) -and $zoneDamage[$sid].Count -gt 0) {
        # Print the table for this zone session
        Write-Host ("===== Zone: {0} (visit #{1}) =====" -f $name, $visit)
        Write-Host ""
        Print-DamageTable -PlayerMap $zoneDamage[$sid] -Title $null | Out-Null

        # Build flattened CSV rows (Player, Health, Armor, Total, ZoneName, Visit, SessionId)
        foreach ($player in $zoneDamage[$sid].Keys) {
            $h = [int]$zoneDamage[$sid][$player].HealthDamage
            $a = [int]$zoneDamage[$sid][$player].ArmorDamage
            $t = $h + $a
            $csvRows.Add([PSCustomObject]@{
                Zone        = $name
                ZoneVisit   = $visit
                SessionId   = $sid
                Player      = $player
                HealthDamage= $h
                ArmorDamage = $a
                TotalDamage = $t
            }) | Out-Null
        }
    } else {
        # Compact single-line notification for no-damage zones
        Write-Host ("No damage recorded in {0} (Visit #{1})" -f $name, $visit)
    }
}


# --- Report: Overall (“Every Zone”) -------------------------------------------
Write-Host "===== Overall Damage Summary (Every Zone) ====="
Write-Host ""
Print-DamageTable -PlayerMap $overallDamage -Title $null | Out-Null

$overallTotalFormatted = "{0:N0}" -f $overallTotalDamage
Write-Host ("**Total Damage Across All Players (Every Zone): {0}**" -f $overallTotalFormatted)

# --- Optional CSV export ------------------------------------------------------
if ($ExportCsv) {
    try {
        # If there were no zone rows, add at least overall rows so the file isn’t empty
        if ($csvRows.Count -eq 0 -and $overallDamage.Count -gt 0) {
            foreach ($player in $overallDamage.Keys) {
                $h = [int]$overallDamage[$player].HealthDamage
                $a = [int]$overallDamage[$player].ArmorDamage
                $t = $h + $a
                $csvRows.Add([PSCustomObject]@{
                    Zone        = "ALL"
                    ZoneVisit   = 0
                    SessionId   = 0
                    Player      = $player
                    HealthDamage= $h
                    ArmorDamage = $a
                    TotalDamage = $t
                }) | Out-Null
            }
        }

        $csvRows | Sort-Object SessionId, { $_.TotalDamage } -Descending | Export-Csv -NoTypeInformation -Path $CsvPath
        Write-Host ""
        Write-Host "CSV exported to: $CsvPath"
    }
    catch {
        Write-Warning "Failed to export CSV: $($_.Exception.Message)"
    }
}

# --- End of script ------------------------------------------------------------
