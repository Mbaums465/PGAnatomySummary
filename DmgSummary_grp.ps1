# Define the path to the player.log file
$logFilePath = "C:\Users\USER\AppData\LocalLow\Elder Game\Project Gorgon\Player.log"

# Read the log file content
$logContent = Get-Content -Path $logFilePath

# Initialize hash tables to store damage and fight counts
$playerDamage = @{}
$fightCount = 0

# --- Define player grouping (aliases) ---
$playerGroups = @{
    "Uber Poker"      = "Yder"
    "Yder Poison Bee" = "Yder"
    "Sandstorm"       = "Yder"
   "Addled Figment"  = "Azizah"
    "Lyramis"        = "Lyramr"
   "Summoned Golem Minion"        = "Azizah"
}

# Function to resolve a player to its master group
function Resolve-PlayerName {
    param([string]$name)
    if ($playerGroups.ContainsKey($name)) {
        return $playerGroups[$name]
    } else {
        return $name
    }
}

# Process the log content line by line
foreach ($line in $logContent) {
    if ($line -match "Detailed Analysis:") {
        $fightCount++
    }
    
    if ($line -match "(?<player>\w[\w\s]*): (?<health>\d+) health dmg (?<armor>\d+) armor dmg") {
        $rawPlayer = $matches.player.Trim()
        $player = Resolve-PlayerName $rawPlayer
        $healthDamage = [int]$matches.health
        $armorDamage = [int]$matches.armor
        
        if (-not $playerDamage.ContainsKey($player)) {
            $playerDamage[$player] = @{
                HealthDamage = 0
                ArmorDamage  = 0
            }
        }
        
        $playerDamage[$player].HealthDamage += $healthDamage
        $playerDamage[$player].ArmorDamage  += $armorDamage
    }
}

# Calculate totals
$overallTotalDamage = 0
foreach ($player in $playerDamage.Keys) {
    $totalDamage = $playerDamage[$player].HealthDamage + $playerDamage[$player].ArmorDamage
    $playerDamage[$player] | Add-Member -MemberType NoteProperty -Name "TotalDamage" -Value $totalDamage
    $overallTotalDamage += $totalDamage
}

# Sort by total damage
$sortedPlayers = $playerDamage.Keys | Sort-Object { $playerDamage[$_].TotalDamage } -Descending

# Output the report
Write-Host "### Damage Report for $fightCount Fights"
Write-Host ""
Write-Host ("| {0,-4} | {1,-20} | {2,-12} | {3,-10} | {4,-13} | {5,-12} |" -f "Rank", "Player", "Total Damage", "% of Total", "Health Damage", "Armor Damage")
Write-Host ("| {0,-4} | {1,-20} | {2,-12} | {3,-10} | {4,-13} | {5,-12} |" -f "----", "--------------------", "------------", "----------", "-------------", "------------")

$rank = 1
foreach ($player in $sortedPlayers) {
    $health = $playerDamage[$player].HealthDamage
    $armor  = $playerDamage[$player].ArmorDamage
    $total  = $playerDamage[$player].TotalDamage
    
    if ($overallTotalDamage -gt 0) {
        $percentage = [math]::Round(($total / $overallTotalDamage) * 100, 2)
        $percentageFormatted = "{0:F2}%" -f $percentage
    } else {
        $percentageFormatted = "0.00%"
    }
    
    $healthFormatted = "{0:N0}" -f $health
    $armorFormatted  = "{0:N0}" -f $armor
    $totalFormatted  = "{0:N0}" -f $total
    
    Write-Host ("| {0,-4} | {1,-20} | {2,12} | {3,10} | {4,13} | {5,12} |" -f $rank, $player, $totalFormatted, $percentageFormatted, $healthFormatted, $armorFormatted)
    $rank++
}

Write-Host ""
$overallTotalFormatted = "{0:N0}" -f $overallTotalDamage
Write-Host "**Total Damage Across All Players: $overallTotalFormatted**"
