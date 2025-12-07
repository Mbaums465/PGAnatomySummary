#!/usr/bin/env python3
"""
Project Gorgon Damage Parser v7
A comprehensive damage tracking tool with GUI and in-memory pandas storage.

Features:
- Real-time log file monitoring (auto-starts on launch)
- In-memory pandas DataFrames (no database locking issues)
- Player alias management with search filter (players only, no monsters)
- Alias grouping - players with same alias have damage combined
- Multiple aggregation views (current zone, rolling window, zone runs)
- DPS calculated per combat encounter (first kill to last kill)
- Multi-select zones in overview for aggregated damage
- Sortable columns in all tables
- Import historical logs (load any backed up player.log file)
- Timezone offset setting (default: EST)

v7 Changes from v6:
- Replaced SQLite with pandas DataFrames (no more database locking)
- Data is kept in memory (temporary by design)
- Import any backed up player.log file for historical viewing
- Uses list-based accumulation for O(1) inserts, lazy DataFrame building
"""

import os
import re
import threading
import time
import tkinter as tk
from tkinter import ttk, filedialog, messagebox, simpledialog
from datetime import datetime, timedelta
from dataclasses import dataclass
from typing import Optional, Dict, List, Tuple, Set
import queue
import json

try:
    import pandas as pd
except ImportError:
    print("ERROR: pandas is required. Install with: pip install pandas")
    raise

# ============================================================================
# CONFIGURATION
# ============================================================================

DEFAULT_LOG_PATH = os.path.expandvars(
    r'C:\Users\%USERNAME%\AppData\LocalLow\Elder Game\Project Gorgon\Player.log'
)

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
CONFIG_PATH = os.path.join(SCRIPT_DIR, 'damage_parser.cfg')
ALIASES_PATH = os.path.join(SCRIPT_DIR, 'damage_parser_aliases.json')

# Regex patterns - compiled once
TIMESTAMP_PATTERN = re.compile(r'^\[(\d{2}:\d{2}:\d{2})\]')
ZONE_LOADING_PATTERN = re.compile(r'^\[(\d{2}:\d{2}:\d{2})\].*LOADING LEVEL (Area\w+)')
ZONE_INIT_PATTERN = re.compile(r'^\[(\d{2}:\d{2}:\d{2})\].*Initializing area!.*:\s*(Area\w+)')
ZONE_C_INIT2_PATTERN = re.compile(r'^\[(\d{2}:\d{2}:\d{2})\].*C_INIT2 for (Area\w+)')
CHARACTER_PATTERN = re.compile(r'Vivox - LoginAsync\((\w+)\)')
CORPSE_PATTERN = re.compile(r'ProcessTalkScreen\((\d+),\s*Search Corpse of ([^,]+),')
DAMAGE_PATTERN = re.compile(
    r'^([^:]+):\s*'
    r'(?:(\d+)\s+health\s+dmg)?'
    r'\s*'
    r'(?:(\d+)\s+armor\s+dmg)?'
    r'(?:.*?Aggro\s*\(at death\):\s*([\d.]+)%)?' # <--- Wrapped in (?: ... )? to make optional
)
WISDOM_PATTERN = re.compile(r'You earned (\d+) Combat Wisdom')

# Zones to skip
SKIP_ZONES = frozenset(['ChooseCharacter', 'ReconnectToServer', 'LoadingScene'])

BATCH_SIZE = 1000  # Larger batches for efficiency

# Timezone offsets
TIMEZONE_OPTIONS = {
    'UTC': 0,
    'EST (UTC-5)': -5,
    'EDT (UTC-4)': -4,
    'CST (UTC-6)': -6,
    'CDT (UTC-5)': -5,
    'MST (UTC-7)': -7,
    'MDT (UTC-6)': -6,
    'PST (UTC-8)': -8,
    'PDT (UTC-7)': -7,
}

# ============================================================================
# UTILITY FUNCTIONS
# ============================================================================

def format_damage_short(value: int) -> str:
    """Format damage as K/M for compact display."""
    if value >= 1_000_000:
        return f"{value / 1_000_000:.1f}M"
    elif value >= 1_000:
        return f"{value / 1_000:.1f}K"
    return str(value)

def truncate_name(name: str, max_len: int = 8) -> str:
    """Truncate name to max_len characters."""
    return name[:max_len] if len(name) > max_len else name

def group_damage_by_alias(data: List[Dict]) -> List[Dict]:
    """Group damage data by display_name (alias or original name)."""
    if not data:
        return []
    
    grouped = {}
    for d in data:
        key = d['display_name']
        if key not in grouped:
            grouped[key] = {
                'display_name': key,
                'player_ids': [],
                'original_names': [],
                'health_dmg': 0,
                'armor_dmg': 0,
                'total_dmg': 0,
                'kills': 0,
                'first_hit': None,
                'last_hit': None
            }
        
        g = grouped[key]
        g['player_ids'].append(d.get('player_id'))
        g['original_names'].append(d.get('original_name', ''))
        g['health_dmg'] += d['health_dmg']
        g['armor_dmg'] += d['armor_dmg']
        g['total_dmg'] += d['total_dmg']
        g['kills'] += d['kills']
        
        if d['first_hit']:
            if g['first_hit'] is None or d['first_hit'] < g['first_hit']:
                g['first_hit'] = d['first_hit']
        if d['last_hit']:
            if g['last_hit'] is None or d['last_hit'] > g['last_hit']:
                g['last_hit'] = d['last_hit']
    
    result = list(grouped.values())
    result.sort(key=lambda x: x['total_dmg'], reverse=True)
    return result

def load_config() -> Dict:
    """Load configuration from file."""
    config = {'timezone': 'EST (UTC-5)'}
    if os.path.exists(CONFIG_PATH):
        try:
            with open(CONFIG_PATH, 'r') as f:
                for line in f:
                    if '=' in line:
                        key, value = line.strip().split('=', 1)
                        config[key] = value
        except Exception:
            pass
    return config

def save_config(config: Dict):
    """Save configuration to file."""
    try:
        with open(CONFIG_PATH, 'w') as f:
            for key, value in config.items():
                f.write(f"{key}={value}\n")
    except Exception:
        pass

def load_aliases() -> Dict[str, str]:
    """Load player aliases from file."""
    if os.path.exists(ALIASES_PATH):
        try:
            with open(ALIASES_PATH, 'r') as f:
                return json.load(f)
        except Exception:
            pass
    return {}

def save_aliases(aliases: Dict[str, str]):
    """Save player aliases to file."""
    try:
        with open(ALIASES_PATH, 'w') as f:
            json.dump(aliases, f, indent=2)
    except Exception:
        pass

# ============================================================================
# DATA CLASSES
# ============================================================================

@dataclass
class DamageEvent:
    player_name: str
    health_dmg: int
    armor_dmg: int
    aggro_percent: float
    npc_id: int
    npc_name: str
    zone_name: str
    timestamp: datetime
    character_name: str
    zone_id: Optional[int] = None

@dataclass
class ZoneInfo:
    name: str
    entered_time: datetime
    character_name: str

# ============================================================================
# PANDAS DATA STORE - List-based accumulation with lazy DataFrame building
# ============================================================================

class PandasDataStore:
    """
    In-memory data storage using pandas DataFrames.
    
    Key optimization: Uses lists for O(1) appends, only builds DataFrames
    when needed for queries. This avoids the O(n²) cost of repeated pd.concat.
    """
    
    def __init__(self):
        self._lock = threading.RLock()
        
        # Primary storage: lists for fast O(1) appends
        self._players_list: List[Dict] = []
        self._zones_list: List[Dict] = []
        self._events_list: List[Dict] = []
        self._wisdom_list: List[dict] = []
        
        # Cached DataFrames - rebuilt lazily when needed
        self._players_df_cache: Optional[pd.DataFrame] = None
        self._zones_df_cache: Optional[pd.DataFrame] = None
        self._events_df_cache: Optional[pd.DataFrame] = None
        
        # Fast lookup indices
        self._player_name_to_id: Dict[str, int] = {}
        self._player_id_to_info: Dict[int, Dict] = {}  # id -> {name, alias}
        self._zone_id_to_info: Dict[int, Dict] = {}  # id -> zone dict
        
        # Aliases (persisted to file)
        self._aliases: Dict[str, str] = load_aliases()
        
        self._next_player_id = 1
        self._next_zone_id = 1
        self._next_event_id = 1
    
    def _invalidate_players_cache(self):
        self._players_df_cache = None
    
    def _invalidate_zones_cache(self):
        self._zones_df_cache = None
    
    def _invalidate_events_cache(self):
        self._events_df_cache = None
    
    def _get_players_df(self) -> pd.DataFrame:
        """Get players DataFrame, rebuilding from list if needed."""
        if self._players_df_cache is None:
            if self._players_list:
                self._players_df_cache = pd.DataFrame(self._players_list)
            else:
                self._players_df_cache = pd.DataFrame(columns=['player_id', 'original_name', 'alias'])
        return self._players_df_cache
    
    def _get_zones_df(self) -> pd.DataFrame:
        """Get zones DataFrame, rebuilding from list if needed."""
        if self._zones_df_cache is None:
            if self._zones_list:
                self._zones_df_cache = pd.DataFrame(self._zones_list)
                if 'entered_time' in self._zones_df_cache.columns:
                    self._zones_df_cache['entered_time'] = pd.to_datetime(self._zones_df_cache['entered_time'])
                if 'left_time' in self._zones_df_cache.columns:
                    self._zones_df_cache['left_time'] = pd.to_datetime(self._zones_df_cache['left_time'])
            else:
                self._zones_df_cache = pd.DataFrame(columns=[
                    'zone_id', 'name', 'character_name', 'entered_time', 'left_time', 'log_date'
                ])
        return self._zones_df_cache
    
    def _get_events_df(self) -> pd.DataFrame:
        """Get events DataFrame, rebuilding from list if needed."""
        if self._events_df_cache is None:
            if self._events_list:
                self._events_df_cache = pd.DataFrame(self._events_list)
                if 'timestamp' in self._events_df_cache.columns:
                    self._events_df_cache['timestamp'] = pd.to_datetime(self._events_df_cache['timestamp'])
            else:
                self._events_df_cache = pd.DataFrame(columns=[
                    'event_id', 'zone_id', 'npc_id', 'npc_name', 'player_id',
                    'health_dmg', 'armor_dmg', 'aggro_percent', 'timestamp', 'character_name'
                ])
        return self._events_df_cache
    
    def get_or_create_player(self, name: str) -> int:
        """Get player ID, creating if needed. O(1) operation."""
        with self._lock:
            if name in self._player_name_to_id:
                return self._player_name_to_id[name]
            
            player_id = self._next_player_id
            self._next_player_id += 1
            
            alias = self._aliases.get(name)
            
            player_dict = {
                'player_id': player_id,
                'original_name': name,
                'alias': alias
            }
            
            self._players_list.append(player_dict)
            self._player_name_to_id[name] = player_id
            self._player_id_to_info[player_id] = {'name': name, 'alias': alias}
            self._invalidate_players_cache()
            
            return player_id
    
    def get_or_create_players_batch(self, names: Set[str]) -> Dict[str, int]:
        """Get/create multiple players at once. All O(1) operations."""
        result = {}
        with self._lock:
            for name in names:
                if name in self._player_name_to_id:
                    result[name] = self._player_name_to_id[name]
                else:
                    player_id = self._next_player_id
                    self._next_player_id += 1
                    
                    alias = self._aliases.get(name)
                    
                    self._players_list.append({
                        'player_id': player_id,
                        'original_name': name,
                        'alias': alias
                    })
                    self._player_name_to_id[name] = player_id
                    self._player_id_to_info[player_id] = {'name': name, 'alias': alias}
                    result[name] = player_id
            
            if result:
                self._invalidate_players_cache()
        
        return result
    
    def update_player_alias(self, player_id: int, alias: str):
        """Update a player's alias."""
        with self._lock:
            if player_id in self._player_id_to_info:
                info = self._player_id_to_info[player_id]
                info['alias'] = alias if alias else None
                original_name = info['name']
                
                for p in self._players_list:
                    if p['player_id'] == player_id:
                        p['alias'] = alias if alias else None
                        break
                
                if alias:
                    self._aliases[original_name] = alias
                elif original_name in self._aliases:
                    del self._aliases[original_name]
                save_aliases(self._aliases)
                
                self._invalidate_players_cache()
    
    def get_all_players(self, filter_text: str = None) -> List[Tuple[int, str, str]]:
        """Get all players who have dealt damage, optionally filtered."""
        with self._lock:
            if not self._events_list:
                return []
            
            events_df = self._get_events_df()
            damage_mask = (events_df['health_dmg'] > 0) | (events_df['armor_dmg'] > 0)
            active_player_ids = set(events_df.loc[damage_mask, 'player_id'].unique())
            
            results = []
            for pid in active_player_ids:
                if pid in self._player_id_to_info:
                    info = self._player_id_to_info[pid]
                    name = info['name']
                    alias = info['alias']
                    
                    if filter_text and filter_text.lower() not in name.lower():
                        continue
                    
                    results.append((pid, name, alias))
            
            results.sort(key=lambda x: x[1])
            return results
    
    def _get_display_name(self, player_id: int) -> str:
        if player_id in self._player_id_to_info:
            info = self._player_id_to_info[player_id]
            return info['alias'] if info['alias'] else info['name']
        return "Unknown"
    
    def _get_original_name(self, player_id: int) -> str:
        if player_id in self._player_id_to_info:
            return self._player_id_to_info[player_id]['name']
        return "Unknown"
    
    def _get_alias(self, player_id: int) -> Optional[str]:
        if player_id in self._player_id_to_info:
            return self._player_id_to_info[player_id]['alias']
        return None
    
    def create_zone_entry(self, name: str, character_name: str, entered_time: datetime, log_date: str = None) -> Optional[int]:
        """Create a new zone entry, but reuse the current one if we are already in the same zone (duplicate log line)."""
        with self._lock:
            # Find the most recent open zone for this character (there should only ever be one)
            open_zone = None
            for i in range(len(self._zones_list) - 1, -1, -1):
                z = self._zones_list[i]
                if z['character_name'] == character_name and z['left_time'] is None:
                    open_zone = z
                    break

            # If we are already in this exact zone right now → do nothing, just return the existing ID (duplicate line)
            if open_zone is not None and open_zone['name'] == name:
                return open_zone['zone_id']

            # We are changing zones → close the previous open zone (if any)
            if open_zone is not None:
                open_zone['left_time'] = entered_time

            # Always create a fresh zone instance for this new visit
            zone_id = self._next_zone_id
            self._next_zone_id += 1

            zone_dict = {
                'zone_id': zone_id,
                'name': name,
                'character_name': character_name,
                'entered_time': entered_time,
                'left_time': None,
                'log_date': log_date
            }

            self._zones_list.append(zone_dict)
            self._zone_id_to_info[zone_id] = zone_dict
            self._invalidate_zones_cache()

            return zone_id
    
    def get_current_zone_id(self, character_name: str) -> Optional[int]:
        """Get the current (open) zone for a character."""
        with self._lock:
            for z in reversed(self._zones_list):
                if z['character_name'] == character_name and z['left_time'] is None:
                    return z['zone_id']
            return None
    
    def insert_damage_event(self, event: DamageEvent, zone_id: int) -> int:
        """Insert a single damage event. O(1) operation."""
        if event.health_dmg == 0 and event.armor_dmg == 0:
            return -1
        
        player_id = self.get_or_create_player(event.player_name)
        
        with self._lock:
            event_id = self._next_event_id
            self._next_event_id += 1
            
            self._events_list.append({
                'event_id': event_id,
                'zone_id': zone_id,
                'npc_id': event.npc_id,
                'npc_name': event.npc_name,
                'player_id': player_id,
                'health_dmg': event.health_dmg,
                'armor_dmg': event.armor_dmg,
                'aggro_percent': event.aggro_percent,
                'timestamp': event.timestamp,
                'character_name': event.character_name
            })
            self._invalidate_events_cache()
            
            return event_id
    
    def insert_damage_events_batch(self, events: List[Tuple]) -> int:
        """Insert multiple damage events at once. O(n) for n events."""
        if not events:
            return 0
        
        with self._lock:
            for e in events:
                zone_id, npc_id, npc_name, player_id, health_dmg, armor_dmg, aggro_pct, timestamp, char_name = e
                event_id = self._next_event_id
                self._next_event_id += 1
                
                self._events_list.append({
                    'event_id': event_id,
                    'zone_id': zone_id,
                    'npc_id': npc_id,
                    'npc_name': npc_name,
                    'player_id': player_id,
                    'health_dmg': health_dmg,
                    'armor_dmg': armor_dmg,
                    'aggro_percent': aggro_pct,
                    'timestamp': timestamp,
                    'character_name': char_name
                })
            
            self._invalidate_events_cache()
            return len(events)
    
    def get_all_existing_event_keys(self, log_date: str = None) -> Set[Tuple]:
        """Get all existing event signatures for deduplication."""
        with self._lock:
            if not self._events_list:
                return set()
            
            if log_date:
                zone_ids = {z['zone_id'] for z in self._zones_list if z['log_date'] == log_date}
                return {
                    (e['zone_id'], e['npc_id'], e['player_id'], e['health_dmg'], e['armor_dmg'])
                    for e in self._events_list if e['zone_id'] in zone_ids
                }
            return {
                (e['zone_id'], e['npc_id'], e['player_id'], e['health_dmg'], e['armor_dmg'])
                for e in self._events_list
            }
    
    def add_wisdom(self, zone_id: int, amount: int):
        with self._lock:
            self._wisdom_list.append({"zone_id": zone_id, "amount": amount})
    
    def get_damage_by_zone(self, zone_id: int) -> List[Dict]:
        """Get damage data for a specific zone."""
        with self._lock:
            events_df = self._get_events_df()
            if len(events_df) == 0:
                return []
            
            mask = (
                (events_df['zone_id'] == zone_id) &
                ((events_df['health_dmg'] > 0) | (events_df['armor_dmg'] > 0))
            )
            if not mask.any():
                return []
            
            filtered = events_df.loc[mask]
            grouped = filtered.groupby('player_id').agg({
                'health_dmg': 'sum',
                'armor_dmg': 'sum',
                'npc_id': 'nunique',
                'timestamp': ['min', 'max']
            }).reset_index()
            
            grouped.columns = ['player_id', 'health_dmg', 'armor_dmg', 'kills', 'first_hit', 'last_hit']
            grouped['total_dmg'] = grouped['health_dmg'] + grouped['armor_dmg']
            grouped = grouped.sort_values('total_dmg', ascending=False)
            
            results = []
            for _, row in grouped.iterrows():
                pid = int(row['player_id'])
                results.append({
                    'player_id': pid,
                    'original_name': self._get_original_name(pid),
                    'alias': self._get_alias(pid),
                    'display_name': self._get_display_name(pid),
                    'health_dmg': int(row['health_dmg']),
                    'armor_dmg': int(row['armor_dmg']),
                    'total_dmg': int(row['total_dmg']),
                    'kills': int(row['kills']),
                    'first_hit': row['first_hit'].to_pydatetime() if pd.notna(row['first_hit']) else None,
                    'last_hit': row['last_hit'].to_pydatetime() if pd.notna(row['last_hit']) else None
                })
            
            return results
    
    def get_damage_by_zones(self, zone_ids: List[int]) -> List[Dict]:
        """Get aggregated damage for multiple zones."""
        if not zone_ids:
            return []
        
        with self._lock:
            events_df = self._get_events_df()
            if len(events_df) == 0:
                return []
            
            mask = (
                (events_df['zone_id'].isin(zone_ids)) &
                ((events_df['health_dmg'] > 0) | (events_df['armor_dmg'] > 0))
            )
            if not mask.any():
                return []
            
            filtered = events_df.loc[mask]
            grouped = filtered.groupby('player_id').agg({
                'health_dmg': 'sum',
                'armor_dmg': 'sum',
                'npc_id': 'nunique',
                'timestamp': ['min', 'max']
            }).reset_index()
            
            grouped.columns = ['player_id', 'health_dmg', 'armor_dmg', 'kills', 'first_hit', 'last_hit']
            grouped['total_dmg'] = grouped['health_dmg'] + grouped['armor_dmg']
            grouped = grouped.sort_values('total_dmg', ascending=False)
            
            results = []
            for _, row in grouped.iterrows():
                pid = int(row['player_id'])
                results.append({
                    'player_id': pid,
                    'original_name': self._get_original_name(pid),
                    'alias': self._get_alias(pid),
                    'display_name': self._get_display_name(pid),
                    'health_dmg': int(row['health_dmg']),
                    'armor_dmg': int(row['armor_dmg']),
                    'total_dmg': int(row['total_dmg']),
                    'kills': int(row['kills']),
                    'first_hit': row['first_hit'].to_pydatetime() if pd.notna(row['first_hit']) else None,
                    'last_hit': row['last_hit'].to_pydatetime() if pd.notna(row['last_hit']) else None
                })
            
            return results
    
    def get_zone_combat_times(self, zone_id: int) -> Tuple[Optional[datetime], Optional[datetime], int]:
        """Get first kill, last kill, and kill count for a zone."""
        with self._lock:
            events_df = self._get_events_df()
            if len(events_df) == 0:
                return None, None, 0
            
            mask = (
                (events_df['zone_id'] == zone_id) &
                ((events_df['health_dmg'] > 0) | (events_df['armor_dmg'] > 0))
            )
            if not mask.any():
                return None, None, 0
            
            filtered = events_df.loc[mask]
            first_hit = filtered['timestamp'].min()
            last_hit = filtered['timestamp'].max()
            kills = filtered['npc_id'].nunique()
            
            return (
                first_hit.to_pydatetime() if pd.notna(first_hit) else None,
                last_hit.to_pydatetime() if pd.notna(last_hit) else None,
                int(kills)
            )
    
    def get_zones_combat_times(self, zone_ids: List[int]) -> Tuple[Optional[datetime], Optional[datetime], int]:
        """Get combat times for multiple zones combined."""
        if not zone_ids:
            return None, None, 0
        
        with self._lock:
            events_df = self._get_events_df()
            if len(events_df) == 0:
                return None, None, 0
            
            mask = (
                (events_df['zone_id'].isin(zone_ids)) &
                ((events_df['health_dmg'] > 0) | (events_df['armor_dmg'] > 0))
            )
            if not mask.any():
                return None, None, 0
            
            filtered = events_df.loc[mask]
            first_hit = filtered['timestamp'].min()
            last_hit = filtered['timestamp'].max()
            kills = filtered['npc_id'].nunique()
            
            return (
                first_hit.to_pydatetime() if pd.notna(first_hit) else None,
                last_hit.to_pydatetime() if pd.notna(last_hit) else None,
                int(kills)
            )
    
    def get_latest_damage_timestamp(self) -> Optional[datetime]:
        """Get the most recent damage timestamp."""
        with self._lock:
            events_df = self._get_events_df()
            if len(events_df) == 0:
                return None
            
            mask = (events_df['health_dmg'] > 0) | (events_df['armor_dmg'] > 0)
            if not mask.any():
                return None
            
            latest = events_df.loc[mask, 'timestamp'].max()
            return latest.to_pydatetime() if pd.notna(latest) else None
    
    def get_damage_in_time_range(self, start_time: datetime, end_time: datetime,
                                 character_name: str = None) -> List[Dict]:
        """Get damage between two timestamps."""
        with self._lock:
            events_df = self._get_events_df()
            if len(events_df) == 0:
                return []
            
            mask = (
                (events_df['timestamp'] >= start_time) &
                (events_df['timestamp'] <= end_time) &
                ((events_df['health_dmg'] > 0) | (events_df['armor_dmg'] > 0))
            )
            if character_name:
                mask = mask & (events_df['character_name'] == character_name)
            
            if not mask.any():
                return []
            
            filtered = events_df.loc[mask]
            grouped = filtered.groupby('player_id').agg({
                'health_dmg': 'sum',
                'armor_dmg': 'sum',
                'npc_id': 'nunique',
                'timestamp': ['min', 'max']
            }).reset_index()
            
            grouped.columns = ['player_id', 'health_dmg', 'armor_dmg', 'kills', 'first_hit', 'last_hit']
            grouped['total_dmg'] = grouped['health_dmg'] + grouped['armor_dmg']
            grouped = grouped.sort_values('total_dmg', ascending=False)
            
            results = []
            for _, row in grouped.iterrows():
                pid = int(row['player_id'])
                results.append({
                    'player_id': pid,
                    'original_name': self._get_original_name(pid),
                    'alias': self._get_alias(pid),
                    'display_name': self._get_display_name(pid),
                    'health_dmg': int(row['health_dmg']),
                    'armor_dmg': int(row['armor_dmg']),
                    'total_dmg': int(row['total_dmg']),
                    'kills': int(row['kills']),
                    'first_hit': row['first_hit'].to_pydatetime() if pd.notna(row['first_hit']) else None,
                    'last_hit': row['last_hit'].to_pydatetime() if pd.notna(row['last_hit']) else None
                })
            
            return results
    
    def _get_zone_stats(self, zone_id: int) -> Dict:
        zone_events = [e for e in self._events_list if e['zone_id'] == zone_id and (e['health_dmg'] > 0 or e['armor_dmg'] > 0)]
        if not zone_events:
            return {'kills': 0, 'total_dmg': 0, 'wisdom': 0}
        kills = len({e['npc_id'] for e in zone_events})
        total_dmg = sum(e['health_dmg'] + e['armor_dmg'] for e in zone_events)
        wisdom = sum(item['amount'] for item in self._wisdom_list if item['zone_id'] == zone_id)
        return {'kills': kills, 'total_dmg': total_dmg, 'wisdom': wisdom}
    
    def get_all_zone_instances(self, zone_name: str = None, log_date: str = None) -> List[Dict]:
        """Get all zone instances, optionally filtered."""
        with self._lock:
            results = []
            for z in reversed(self._zones_list):
                if zone_name and z['name'] != zone_name:
                    continue
                if log_date and z['log_date'] != log_date:
                    continue
                results.append({
                    'zone_id': z['zone_id'],
                    'name': z['name'],
                    'character_name': z['character_name'],
                    'entered_time': z['entered_time'],
                    'left_time': z['left_time'],
                    'log_date': z['log_date']
                })
            return results
    
    def get_unique_zone_names(self) -> List[str]:
        with self._lock:
            return sorted(set(z['name'] for z in self._zones_list))
    
    def get_unique_log_dates(self) -> List[str]:
        with self._lock:
            dates = set(z['log_date'] for z in self._zones_list if z['log_date'])
            return sorted(dates, reverse=True)
    
    def clear_all_data(self):
        """Clear all data (start fresh)."""
        with self._lock:
            self._players_list.clear()
            self._zones_list.clear()
            self._events_list.clear()
            self._wisdom_list.clear()
            
            self._players_df_cache = None
            self._zones_df_cache = None
            self._events_df_cache = None
            
            self._player_name_to_id.clear()
            self._player_id_to_info.clear()
            self._zone_key_to_id.clear()
            self._zone_id_to_info.clear()
            
            self._next_player_id = 1
            self._next_zone_id = 1
            self._next_event_id = 1
    
    def get_stats(self) -> Dict:
        with self._lock:
            return {
                'zones': len(self._zones_list),
                'events': len(self._events_list),
                'players': len(self._players_list)
            }


# ============================================================================
# LOG PARSER
# ============================================================================

class LogParser:
    def __init__(self, data_store: PandasDataStore, event_queue: queue.Queue):
        self.data_store = data_store
        self.event_queue = event_queue
        self.current_character: Optional[str] = None
        self.current_zone: Optional[ZoneInfo] = None
        self.current_zone_id: Optional[int] = None
        self.current_date: datetime = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
        self.last_timestamp: Optional[datetime] = None
        self.pending_corpse: Optional[Tuple[int, str, datetime]] = None
        self.log_date: Optional[str] = None
        
        self.batch_mode = False
        self.pending_events: List[DamageEvent] = []
        self.seen_events: Set[Tuple] = set()
        self.zones_created: Dict[Tuple, int] = {}
        
        self.last_zone_name: Optional[str] = None
        self.last_zone_time: Optional[datetime] = None
        self.zone_debounce_seconds = 30
    
    def set_log_date(self, date_str: str):
        self.log_date = date_str
        try:
            self.current_date = datetime.strptime(date_str, '%Y-%m-%d')
        except Exception:
            pass
    
    def reset(self):
        self.current_character = None
        self.current_zone = None
        self.current_zone_id = None
        self.last_timestamp = None
        self.pending_corpse = None
        self.pending_events.clear()
        self.seen_events.clear()
        self.zones_created.clear()
        self.last_zone_name = None
        self.last_zone_time = None
    
    def start_batch_mode(self, existing_events: Set[Tuple] = None):
        self.batch_mode = True
        self.pending_events.clear()
        self.seen_events = existing_events or set()
        self.zones_created.clear()
    
    def end_batch_mode(self) -> int:
        self.batch_mode = False
        count = self._flush_batch()
        self.pending_events.clear()
        return count
    
    def _flush_batch(self) -> int:
        if not self.pending_events:
            return 0
        
        player_names = {e.player_name for e in self.pending_events}
        player_ids = self.data_store.get_or_create_players_batch(player_names)
        
        events_to_insert = []
        for e in self.pending_events:
            player_id = player_ids.get(e.player_name)
            if not player_id:
                continue
            
            zone_id = e.zone_id
            if not zone_id:
                zone_key = (e.zone_name, e.character_name)
                cached_zone_id = self.zones_created.get(zone_key)
                if cached_zone_id:
                    zone_id = cached_zone_id
                else:
                    zone_id = self.data_store.create_zone_entry(
                        e.zone_name, e.character_name, e.timestamp, self.log_date
                    )
                    self.zones_created[zone_key] = zone_id
            
            if not zone_id:
                continue
            
            dedup_key = (zone_id, e.npc_id, player_id, e.health_dmg, e.armor_dmg)
            if dedup_key in self.seen_events:
                continue
            self.seen_events.add(dedup_key)
            
            events_to_insert.append((
                zone_id, e.npc_id, e.npc_name, player_id, e.health_dmg, e.armor_dmg,
                e.aggro_percent, e.timestamp, e.character_name
            ))
        
        count = self.data_store.insert_damage_events_batch(events_to_insert)
        self.pending_events.clear()
        return count
    
    def parse_timestamp(self, time_str: str) -> datetime:
        time_obj = datetime.strptime(time_str, '%H:%M:%S').time()
        result = datetime.combine(self.current_date.date(), time_obj)
        
        if self.last_timestamp and result < self.last_timestamp:
            self.current_date += timedelta(days=1)
            result = datetime.combine(self.current_date.date(), time_obj)
        
        self.last_timestamp = result
        return result
    
    def parse_line(self, line: str) -> Optional[DamageEvent]:
        line = line.strip()
        if not line:
            return None
        
        char_match = CHARACTER_PATTERN.search(line)
        if char_match:
            self.current_character = char_match.group(1)
            if not self.batch_mode:
                self.event_queue.put(('character', self.current_character))
            return None
        
        for pattern in [ZONE_LOADING_PATTERN, ZONE_INIT_PATTERN, ZONE_C_INIT2_PATTERN]:
            zone_match = pattern.search(line)
            if zone_match:
                time_str, zone_name = zone_match.groups()
                if zone_name in SKIP_ZONES:
                    continue
                timestamp = self.parse_timestamp(time_str)
                
                if (self.last_zone_name == zone_name and 
                    self.last_zone_time and
                    (timestamp - self.last_zone_time).total_seconds() < self.zone_debounce_seconds):
                    self.current_zone = ZoneInfo(zone_name, timestamp, self.current_character)
                    return None
                
                self.current_zone = ZoneInfo(zone_name, timestamp, self.current_character)
                self.last_zone_name = zone_name
                self.last_zone_time = timestamp
                
                zone_id = self.data_store.create_zone_entry(
                    zone_name, self.current_character, timestamp, self.log_date
                )
                self.current_zone_id = zone_id
                
                if self.batch_mode:
                    zone_key = (zone_name, self.current_character)
                    self.zones_created[zone_key] = zone_id
                else:
                    self.event_queue.put(('zone', zone_name, timestamp))
                return None
        
        corpse_match = CORPSE_PATTERN.search(line)
        if corpse_match:
            npc_id = int(corpse_match.group(1))
            npc_name = corpse_match.group(2).strip()
            ts_match = TIMESTAMP_PATTERN.match(line)
            timestamp = self.parse_timestamp(ts_match.group(1)) if ts_match else datetime.now()
            self.pending_corpse = (npc_id, npc_name, timestamp)
            return None
        
        damage_match = DAMAGE_PATTERN.match(line)
        if damage_match and self.pending_corpse:
            npc_id, npc_name, corpse_timestamp = self.pending_corpse
            
            player_name = damage_match.group(1).strip()
            health_dmg = int(damage_match.group(2)) if damage_match.group(2) else 0
            armor_dmg = int(damage_match.group(3)) if damage_match.group(3) else 0
            aggro_str = damage_match.group(4)
            aggro_pct = float(aggro_str) if aggro_str else 0.0  
            
            if health_dmg == 0 and armor_dmg == 0:
                return None
            
            event = DamageEvent(
                player_name=player_name,
                health_dmg=health_dmg,
                armor_dmg=armor_dmg,
                aggro_percent=aggro_pct,
                npc_id=npc_id,
                npc_name=npc_name,
                zone_name=self.current_zone.name if self.current_zone else "Unknown",
                timestamp=corpse_timestamp,
                character_name=self.current_character,
                zone_id=self.current_zone_id
            )
            
            if self.batch_mode:
                self.pending_events.append(event)
                if len(self.pending_events) >= BATCH_SIZE:
                    self._flush_batch()
            else:
                result = self.data_store.insert_damage_event(event, self.current_zone_id)
                if result != -1:
                    self.event_queue.put(('damage', event))
            
            return event
        
        wisdom_match = WISDOM_PATTERN.search(line)
        if wisdom_match and self.current_zone_id:
            amount = int(wisdom_match.group(1))
            self.data_store.add_wisdom(self.current_zone_id, amount)
            return None
        
        return None


# ============================================================================
# LOG MONITOR
# ============================================================================

class LogMonitor:
    def __init__(self, log_path: str, parser: LogParser):
        self.log_path = log_path
        self.parser = parser
        self.running = False
        self.thread: Optional[threading.Thread] = None
        self.file_position = 0
    
    def start(self, from_position: int = 0):
        if self.running:
            return
        self.running = True
        self.file_position = from_position
        self.thread = threading.Thread(target=self._monitor_loop, daemon=True)
        self.thread.start()
    
    def stop(self):
        self.running = False
        if self.thread:
            self.thread.join(timeout=2)
    
    def _monitor_loop(self):
        try:
            if not os.path.exists(self.log_path):
                self.parser.event_queue.put(('error', f"Log file not found: {self.log_path}"))
                return
            
            with open(self.log_path, 'r', encoding='utf-8', errors='ignore') as f:
                f.seek(self.file_position)
                
                while self.running:
                    line = f.readline()
                    if line:
                        try:
                            self.parser.parse_line(line)
                        except Exception:
                            pass
                        self.file_position = f.tell()
                    else:
                        time.sleep(0.1)
        except Exception as e:
            self.parser.event_queue.put(('error', str(e)))


# ============================================================================
# BACKGROUND FILE LOADER
# ============================================================================

class BackgroundLoader:
    """Loads log files in background thread with progress reporting."""
    
    def __init__(self, data_store: PandasDataStore, progress_callback=None, complete_callback=None):
        self.data_store = data_store
        self.progress_callback = progress_callback
        self.complete_callback = complete_callback
        self.thread: Optional[threading.Thread] = None
        self.cancel_requested = False
    
    def load_file(self, log_path: str, log_date: str):
        self.cancel_requested = False
        self.thread = threading.Thread(
            target=self._load_worker,
            args=(log_path, log_date),
            daemon=True
        )
        self.thread.start()
    
    def cancel(self):
        self.cancel_requested = True
    
    def _load_worker(self, log_path: str, log_date: str):
        try:
            file_size = os.path.getsize(log_path)
            
            temp_queue = queue.Queue()
            parser = LogParser(self.data_store, temp_queue)
            parser.set_log_date(log_date)
            
            if self.progress_callback:
                self.progress_callback(0, "Checking for duplicates...")
            existing_events = self.data_store.get_all_existing_event_keys(log_date)
            
            parser.start_batch_mode(existing_events)
            
            if self.progress_callback:
                self.progress_callback(0, "Loading log file...")
            
            line_count = 0
            damage_count = 0
            last_progress = 0
            current_char = None
            current_zone = None
            
            with open(log_path, 'r', encoding='utf-8', errors='ignore') as f:
                while True:
                    if self.cancel_requested:
                        if self.complete_callback:
                            self.complete_callback(False, "Cancelled", 0, 0, None, None)
                        return
                    
                    line = f.readline()
                    if not line:
                        break
                    
                    line_count += 1
                    try:
                        event = parser.parse_line(line)
                        if event:
                            damage_count += 1
                        
                        if parser.current_character != current_char:
                            current_char = parser.current_character
                        if parser.current_zone and parser.current_zone.name != current_zone:
                            current_zone = parser.current_zone.name
                    except Exception:
                        pass
                    
                    if line_count % 10000 == 0:
                        pos = f.tell()
                        progress = int((pos / file_size) * 100)
                        if progress != last_progress and self.progress_callback:
                            self.progress_callback(
                                progress, 
                                f"Processing... {line_count:,} lines, {damage_count:,} damage events"
                            )
                            last_progress = progress
            
            if self.progress_callback:
                self.progress_callback(95, "Finalizing...")
            
            final_count = parser.end_batch_mode()
            damage_count += final_count
            
            if self.progress_callback:
                self.progress_callback(100, "Complete!")
            
            if self.complete_callback:
                self.complete_callback(
                    True, 
                    f"Loaded {line_count:,} lines, {damage_count:,} damage events",
                    line_count,
                    damage_count,
                    current_char,
                    current_zone
                )
        except Exception as e:
            if self.complete_callback:
                self.complete_callback(False, f"Error: {str(e)}", 0, 0, None, None)


# ============================================================================
# SORTABLE TREEVIEW HELPER
# ============================================================================

def make_treeview_sortable(tree: ttk.Treeview, preserve_selection: bool = False):
    """Make all columns in a treeview sortable by clicking headers."""
    def sort_column(col, reverse):
        saved_selection = tree.selection() if preserve_selection else ()
        items = [(tree.set(k, col), k) for k in tree.get_children('')]
        
        def parse_val(v):
            v = v.replace(',', '').replace('%', '').replace('★', '').strip()
            if v in ('--', '(current)', '', '(no alias)'):
                return float('-inf') if not reverse else float('inf')
            try:
                return float(v)
            except ValueError:
                return v.lower() if isinstance(v, str) else v
        
        try:
            items.sort(key=lambda t: parse_val(t[0]), reverse=reverse)
        except TypeError:
            items.sort(key=lambda t: str(t[0]), reverse=reverse)
        
        for index, (val, k) in enumerate(items):
            tree.move(k, '', index)
        
        if preserve_selection and saved_selection:
            for item in saved_selection:
                if tree.exists(item):
                    tree.selection_add(item)
        
        tree.heading(col, command=lambda: sort_column(col, not reverse))
    
    for col in tree['columns']:
        tree.heading(col, command=lambda c=col: sort_column(c, False))


# ============================================================================
# GUI APPLICATION
# ============================================================================

class DamageParserGUI:
    def __init__(self, log_path: str = None):
        self.root = tk.Tk()
        self.root.title("Project Gorgon Damage Parser v7 (Pandas)")
        self.root.geometry("1300x850")
        
        self.log_path = log_path or DEFAULT_LOG_PATH
        
        self.data_store = PandasDataStore()
        self.event_queue = queue.Queue()
        self.parser = LogParser(self.data_store, self.event_queue)
        self.monitor: Optional[LogMonitor] = None
        self.loader: Optional[BackgroundLoader] = None
        
        self.current_character = None
        self.current_zone = None
        self.current_zone_id = None
        self.monitoring_active = False
        self.loading_active = False
        
        self.config = load_config()
        self.timezone_var = tk.StringVar(value=self.config.get('timezone', 'EST (UTC-5)'))
        
        self.mini_window = None
        
        self._create_menu()
        self._create_main_layout()
        self._start_event_processor()
        self._start_auto_refresh()
        
        self.root.after(100, self._auto_start)
    
    def _get_tz_offset(self) -> int:
        tz = self.timezone_var.get()
        return TIMEZONE_OPTIONS.get(tz, -5)
    
    def _apply_tz(self, dt: Optional[datetime]) -> Optional[datetime]:
        if dt is None:
            return None
        return dt + timedelta(hours=self._get_tz_offset())
    
    def _format_time(self, dt: Optional[datetime]) -> str:
        if dt is None:
            return "--"
        adjusted = self._apply_tz(dt)
        return adjusted.strftime('%H:%M:%S')
    
    def _auto_start(self):
        if os.path.exists(self.log_path):
            self._add_feed_line("Auto-loading player.log (full file)...", 'character')
            self._load_file_background(self.log_path, monitor_after=True)
        else:
            self._add_feed_line(f"Player.log not found at: {self.log_path}", 'error')
            self._add_feed_line("Use File > Import Log File to load a log.", 'character')
    
    def _create_menu(self):
        menubar = tk.Menu(self.root)
        self.root.config(menu=menubar)
        
        file_menu = tk.Menu(menubar, tearoff=0)
        menubar.add_cascade(label="File", menu=file_menu)
        file_menu.add_command(label="Import Log File...", command=self._import_log_file)
        file_menu.add_command(label="Export to CSV...", command=self._export_csv)
        file_menu.add_separator()
        file_menu.add_command(label="Exit", command=self._on_close)
        
        session_menu = tk.Menu(menubar, tearoff=0)
        menubar.add_cascade(label="Session", menu=session_menu)
        session_menu.add_command(label="Clear All Data", command=self._clear_all_data)
        
        view_menu = tk.Menu(menubar, tearoff=0)
        menubar.add_cascade(label="View", menu=view_menu)
        
        tz_menu = tk.Menu(view_menu, tearoff=0)
        view_menu.add_cascade(label="Timezone", menu=tz_menu)
        for tz_name in TIMEZONE_OPTIONS.keys():
            tz_menu.add_radiobutton(label=tz_name, variable=self.timezone_var, 
                                    value=tz_name, command=self._on_timezone_changed)
    
    def _on_timezone_changed(self):
        self.config['timezone'] = self.timezone_var.get()
        save_config(self.config)
        self._add_feed_line(f"Timezone changed to {self.timezone_var.get()}", 'character')
        self._refresh_zone_runs()
        self.tz_label.config(text=f"TZ: {self.timezone_var.get()}")
    
    def _create_main_layout(self):
        control_frame = ttk.Frame(self.root, padding="5")
        control_frame.pack(fill='x')
        
        self.monitor_btn = ttk.Button(control_frame, text="▶ Start Monitoring", 
                                      command=self._toggle_monitoring)
        self.monitor_btn.pack(side='left', padx=5)
        
        ttk.Button(control_frame, text="Import Log...", command=self._import_log_file).pack(side='left', padx=5)
        ttk.Button(control_frame, text="📊 Mini View", command=self._open_mini_window).pack(side='left', padx=5)
        
        self.char_label = ttk.Label(control_frame, text="Character: --")
        self.char_label.pack(side='left', padx=20)
        
        self.zone_label = ttk.Label(control_frame, text="Zone: --")
        self.zone_label.pack(side='left', padx=20)
        
        self.log_date_label = ttk.Label(control_frame, text="Log Date: --")
        self.log_date_label.pack(side='left', padx=20)
        
        self.tz_label = ttk.Label(control_frame, text=f"TZ: {self.timezone_var.get()}")
        self.tz_label.pack(side='left', padx=10)
        
        self.status_label = ttk.Label(control_frame, text="Status: Idle", foreground='red')
        self.status_label.pack(side='right', padx=5)
        
        self.progress_frame = ttk.Frame(self.root)
        self.progress_var = tk.DoubleVar()
        self.progress_bar = ttk.Progressbar(self.progress_frame, variable=self.progress_var, 
                                            maximum=100, length=400)
        self.progress_bar.pack(side='left', padx=5)
        self.progress_label = ttk.Label(self.progress_frame, text="")
        self.progress_label.pack(side='left', padx=10)
        ttk.Button(self.progress_frame, text="Cancel", command=self._cancel_load).pack(side='left', padx=5)
        
        self.notebook = ttk.Notebook(self.root)
        self.notebook.pack(fill='both', expand=True, padx=5, pady=5)
        
        self._create_damage_feed_tab()
        self._create_current_zone_tab()
        self._create_rolling_window_tab()
        self._create_zone_runs_tab()
        self._create_alias_tab()
    
    def _show_progress(self, show: bool):
        if show:
            self.progress_frame.pack(fill='x', padx=5, pady=2, before=self.notebook)
        else:
            self.progress_frame.pack_forget()
    
    def _update_progress(self, percent: int, message: str):
        self.root.after(0, lambda: self._do_update_progress(percent, message))
    
    def _do_update_progress(self, percent: int, message: str):
        self.progress_var.set(percent)
        self.progress_label.config(text=message)
    
    def _cancel_load(self):
        if self.loader:
            self.loader.cancel()
    
    def _create_damage_feed_tab(self):
        frame = ttk.Frame(self.notebook, padding="5")
        self.notebook.add(frame, text="Live Feed")
        
        self.feed_text = tk.Text(frame, wrap='word', height=30, state='disabled',
                                 font=('Consolas', 10))
        scrollbar = ttk.Scrollbar(frame, orient='vertical', command=self.feed_text.yview)
        self.feed_text.configure(yscrollcommand=scrollbar.set)
        
        self.feed_text.pack(side='left', fill='both', expand=True)
        scrollbar.pack(side='right', fill='y')
        
        self.feed_text.tag_configure('zone', foreground='#2196F3', font=('Consolas', 10, 'bold'))
        self.feed_text.tag_configure('damage', foreground='#4CAF50')
        self.feed_text.tag_configure('character', foreground='#FF9800', font=('Consolas', 10, 'bold'))
        self.feed_text.tag_configure('error', foreground='#F44336')
        self.feed_text.tag_configure('group', foreground='#9C27B0', font=('Consolas', 10, 'bold'))
    
    def _create_current_zone_tab(self):
        frame = ttk.Frame(self.notebook, padding="5")
        self.notebook.add(frame, text="Current Zone")
        
        self.zone_info_label = ttk.Label(frame, text="Zone: -- | Combat Time: -- | DPS = Damage / Combat Duration")
        self.zone_info_label.pack(fill='x', pady=5)
        
        columns = ('Player', 'Health Dmg', 'Armor Dmg', 'Total Dmg', 'DPS', '% of Group', 'Kills')
        self.zone_tree = ttk.Treeview(frame, columns=columns, show='headings', height=20)
        
        for col in columns:
            self.zone_tree.heading(col, text=col)
            self.zone_tree.column(col, width=100, anchor='center')
        self.zone_tree.column('Player', width=180, anchor='w')
        
        make_treeview_sortable(self.zone_tree)
        
        zone_scroll = ttk.Scrollbar(frame, orient='vertical', command=self.zone_tree.yview)
        self.zone_tree.configure(yscrollcommand=zone_scroll.set)
        
        self.zone_tree.pack(side='left', fill='both', expand=True)
        zone_scroll.pack(side='right', fill='y')
    
    def _create_rolling_window_tab(self):
        frame = ttk.Frame(self.notebook, padding="5")
        self.notebook.add(frame, text="Last 5 Minutes")
        
        window_frame = ttk.Frame(frame)
        window_frame.pack(fill='x', pady=5)
        
        ttk.Label(window_frame, text="Window (minutes):").pack(side='left')
        self.window_var = tk.StringVar(value="5")
        ttk.Entry(window_frame, textvariable=self.window_var, width=5).pack(side='left', padx=5)
        ttk.Button(window_frame, text="Refresh", command=self._refresh_rolling_window).pack(side='left', padx=5)
        
        self.rolling_info_label = ttk.Label(window_frame, text="")
        self.rolling_info_label.pack(side='left', padx=20)
        
        columns = ('Player', 'Health Dmg', 'Armor Dmg', 'Total Dmg', 'DPS', '% of Group', 'Kills')
        self.rolling_tree = ttk.Treeview(frame, columns=columns, show='headings', height=20)
        
        for col in columns:
            self.rolling_tree.heading(col, text=col)
            self.rolling_tree.column(col, width=100, anchor='center')
        self.rolling_tree.column('Player', width=180, anchor='w')
        
        make_treeview_sortable(self.rolling_tree)
        
        rolling_scroll = ttk.Scrollbar(frame, orient='vertical', command=self.rolling_tree.yview)
        self.rolling_tree.configure(yscrollcommand=rolling_scroll.set)
        
        self.rolling_tree.pack(side='left', fill='both', expand=True)
        rolling_scroll.pack(side='right', fill='y')
    
    def _create_zone_runs_tab(self):
        frame = ttk.Frame(self.notebook, padding="5")
        self.notebook.add(frame, text="Zone Runs")

        filter_frame = ttk.LabelFrame(frame, text="Filters", padding="5")
        filter_frame.pack(fill='x', pady=5)

        ttk.Label(filter_frame, text="Zone:").pack(side='left')
        self.zone_filter_var = tk.StringVar(value="(All)")
        self.zone_combo = ttk.Combobox(filter_frame, textvariable=self.zone_filter_var, width=20, state='readonly')
        self.zone_combo.pack(side='left', padx=5)

        ttk.Label(filter_frame, text="Date:").pack(side='left', padx=(20,0))
        self.date_filter_var = tk.StringVar(value="(All)")
        self.date_combo = ttk.Combobox(filter_frame, textvariable=self.date_filter_var, width=15, state='readonly')
        self.date_combo.pack(side='left', padx=5)

        ttk.Label(filter_frame, text="Min Wisdom:").pack(side='left', padx=(20,0))
        self.min_wisdom_var = tk.StringVar(value="")
        ttk.Entry(filter_frame, textvariable=self.min_wisdom_var, width=8).pack(side='left', padx=5)

        ttk.Button(filter_frame, text="Refresh", command=self._refresh_zone_runs).pack(side='right', padx=5)

        paned = ttk.PanedWindow(frame, orient='vertical')
        paned.pack(fill='both', expand=True)

        zones_frame = ttk.LabelFrame(paned, text="Zone Runs (Ctrl/Shift for multi-select)", padding="5")
        paned.add(zones_frame, weight=1)

        columns = ('Zone', 'Character', 'Date', 'Entered', 'Left', 'Wisdom', 'Kills', 'Total Damage')
        self.zones_tree = ttk.Treeview(zones_frame, columns=columns, show='headings', selectmode='extended')
        for col in columns:
            self.zones_tree.heading(col, text=col)
            self.zones_tree.column(col, width=100 if col not in ('Wisdom', 'Kills') else 90, anchor='center')
        self.zones_tree.column('Zone', width=150, anchor='w')
        self.zones_tree.column('Character', width=120, anchor='w')
        make_treeview_sortable(self.zones_tree, preserve_selection=True)
        self.zones_tree.pack(side='left', fill='both', expand=True)
        ttk.Scrollbar(zones_frame, orient='vertical', command=self.zones_tree.yview).pack(side='right', fill='y')
        self.zones_tree.bind('<<TreeviewSelect>>', self._on_zone_runs_selection_changed)

        totals_frame = ttk.LabelFrame(paned, text="Damage Totals (selected runs)", padding="5")
        paned.add(totals_frame, weight=2)

        copy_frame = ttk.Frame(totals_frame)
        copy_frame.pack(fill='x', pady=2)
        ttk.Button(copy_frame, text="Copy Full", command=self._copy_zone_runs_full).pack(side='left', padx=2)
        ttk.Button(copy_frame, text="Copy Compact", command=self._copy_zone_runs_compact).pack(side='left', padx=2)

        self.selected_zones_label = ttk.Label(totals_frame, text="No zones selected")
        self.selected_zones_label.pack(fill='x', pady=2)

        columns = ('Player', 'Health Dmg', 'Armor Dmg', 'Total Dmg', 'DPS', '%', 'Kills')
        self.session_tree = ttk.Treeview(totals_frame, columns=columns, show='headings')
        for col in columns:
            self.session_tree.heading(col, text=col)
            self.session_tree.column(col, width=100, anchor='center')
        self.session_tree.column('Player', width=180, anchor='w')
        make_treeview_sortable(self.session_tree)
        self.session_tree.pack(side='left', fill='both', expand=True)
        ttk.Scrollbar(totals_frame, orient='vertical', command=self.session_tree.yview).pack(side='right', fill='y')
    
    def _create_alias_tab(self):
        frame = ttk.Frame(self.notebook, padding="5")
        self.notebook.add(frame, text="Player Aliases")
        
        filter_frame = ttk.Frame(frame)
        filter_frame.pack(fill='x', pady=5)
        
        ttk.Label(filter_frame, text="Filter by name:").pack(side='left')
        self.alias_filter_var = tk.StringVar()
        self.alias_filter_var.trace_add('write', lambda *args: self._refresh_alias_list())
        alias_filter_entry = ttk.Entry(filter_frame, textvariable=self.alias_filter_var, width=30)
        alias_filter_entry.pack(side='left', padx=5)
        
        ttk.Button(filter_frame, text="Refresh", command=self._refresh_alias_list).pack(side='left', padx=10)
        
        ttk.Label(filter_frame, text="(Double-click to edit alias. Same alias = grouped damage. Aliases are saved to file.)").pack(side='left', padx=20)
        
        columns = ('Original Name', 'Alias')
        self.alias_tree = ttk.Treeview(frame, columns=columns, show='headings', height=20)
        
        self.alias_tree.heading('Original Name', text='Original Name')
        self.alias_tree.heading('Alias', text='Alias')
        self.alias_tree.column('Original Name', width=250)
        self.alias_tree.column('Alias', width=250)
        
        make_treeview_sortable(self.alias_tree)
        
        alias_scroll = ttk.Scrollbar(frame, orient='vertical', command=self.alias_tree.yview)
        self.alias_tree.configure(yscrollcommand=alias_scroll.set)
        
        self.alias_tree.pack(side='left', fill='both', expand=True)
        alias_scroll.pack(side='right', fill='y')
        
        self.alias_tree.bind('<Double-1>', self._edit_alias)
    
    def _load_file_background(self, log_path: str, monitor_after: bool = False):
        if self.loading_active:
            messagebox.showwarning("Loading", "Already loading a file. Please wait.")
            return
        
        if not os.path.exists(log_path):
            messagebox.showerror("Error", f"Log file not found:\n{log_path}")
            return
        
        if self.monitor:
            self.monitor.stop()
            self.monitor = None
        
        mtime = os.path.getmtime(log_path)
        log_date = datetime.fromtimestamp(mtime).strftime('%Y-%m-%d')
        self.log_date_label.config(text=f"Log Date: {log_date}")
        
        self.loading_active = True
        self._show_progress(True)
        self.monitor_btn.config(state='disabled')
        self.status_label.config(text="Status: Loading...", foreground='red')
        
        self._add_feed_line(f"Loading: {log_path}", 'character')
        self._add_feed_line(f"Log date: {log_date} (times shown in {self.timezone_var.get()})", 'character')
        
        def on_progress(percent, message):
            self._update_progress(percent, message)
        
        def on_complete(success, message, lines, events, char, zone):
            self.root.after(0, lambda: self._on_load_complete(
                success, message, lines, events, char, zone, log_path, monitor_after
            ))
        
        self.loader = BackgroundLoader(self.data_store, on_progress, on_complete)
        self.loader.load_file(log_path, log_date)
    
    def _on_load_complete(self, success: bool, message: str, lines: int, events: int,
                          char: str, zone: str, log_path: str, monitor_after: bool):
        self.loading_active = False
        self._show_progress(False)
        self.monitor_btn.config(state='normal')
        
        if success:
            self._add_feed_line(message, 'character')
            
            stats = self.data_store.get_stats()
            self._add_feed_line(f"Data loaded: {stats['zones']} zones, {stats['events']} events, {stats['players']} players", 'character')
            
            if char:
                self.current_character = char
                self.char_label.config(text=f"Character: {char}")
            if zone:
                self.current_zone = zone
                self.zone_label.config(text=f"Zone: {zone}")
            
            if monitor_after and log_path == self.log_path:
                file_size = os.path.getsize(log_path)
                
                self.parser.reset()
                mtime = os.path.getmtime(log_path)
                log_date = datetime.fromtimestamp(mtime).strftime('%Y-%m-%d')
                self.parser.set_log_date(log_date)
                self.parser.current_character = self.current_character
                self.parser.current_zone = ZoneInfo(zone, datetime.now(), char) if zone and char else None
                
                if char:
                    zone_id = self.data_store.get_current_zone_id(char)
                    if zone_id:
                        self.parser.current_zone_id = zone_id
                        self.current_zone_id = zone_id
                
                self.monitor = LogMonitor(log_path, self.parser)
                self.monitor.start(from_position=file_size)
                self.monitoring_active = True
                self.monitor_btn.config(text="⏹ Stop Monitoring")
                self.status_label.config(text="Status: Monitoring", foreground='green')
                self._add_feed_line("Now monitoring for new data...", 'character')
            else:
                self.status_label.config(text="Status: Idle", foreground='red')
            
            self._refresh_all()
        else:
            self._add_feed_line(message, 'error')
            self.status_label.config(text="Status: Error", foreground='red')
    
    def _import_log_file(self):
        filepath = filedialog.askopenfilename(
            title="Import Log File",
            filetypes=[("Log files", "*.log"), ("All files", "*.*")],
            initialdir=os.path.dirname(self.log_path) if os.path.exists(os.path.dirname(self.log_path)) else None
        )
        if filepath:
            self._load_file_background(filepath, monitor_after=False)
    
    def _toggle_monitoring(self):
        if self.monitoring_active:
            self._stop_monitoring()
        else:
            self._start_monitoring()
    
    def _start_monitoring(self):
        if not os.path.exists(self.log_path):
            messagebox.showerror("Error", f"Log file not found:\n{self.log_path}")
            return
        
        mtime = os.path.getmtime(self.log_path)
        log_date = datetime.fromtimestamp(mtime).strftime('%Y-%m-%d')
        self.log_date_label.config(text=f"Log Date: {log_date}")
        self.parser.set_log_date(log_date)
        
        if self.current_character:
            self.parser.current_character = self.current_character
            zone_id = self.data_store.get_current_zone_id(self.current_character)
            if zone_id:
                self.parser.current_zone_id = zone_id
                self.current_zone_id = zone_id
                if self.current_zone:
                    self.parser.current_zone = ZoneInfo(
                        self.current_zone, datetime.now(), self.current_character
                    )
        
        file_size = os.path.getsize(self.log_path)
        self.monitor = LogMonitor(self.log_path, self.parser)
        self.monitor.start(from_position=file_size)
        
        self.monitoring_active = True
        self.monitor_btn.config(text="⏹ Stop Monitoring")
        self.status_label.config(text="Status: Monitoring", foreground='green')
        self._add_feed_line(f"Monitoring: {self.log_path}", 'character')
    
    def _stop_monitoring(self):
        if self.monitor:
            self.monitor.stop()
            self.monitor = None
        
        self.monitoring_active = False
        self.monitor_btn.config(text="▶ Start Monitoring")
        self.status_label.config(text="Status: Idle", foreground='red')
        self._add_feed_line("Monitoring stopped", 'error')
    
    def _start_event_processor(self):
        def process():
            try:
                while True:
                    event = self.event_queue.get_nowait()
                    self._handle_event(event)
            except queue.Empty:
                pass
            self.root.after(100, process)
        self.root.after(100, process)
    
    def _handle_event(self, event):
        event_type = event[0]
        
        if event_type == 'character':
            self.current_character = event[1]
            self.char_label.config(text=f"Character: {event[1]}")
            self._add_feed_line(f"Character: {event[1]}", 'character')
            
        elif event_type == 'zone':
            zone_name, timestamp = event[1], event[2]
            self.current_zone = zone_name
            self.current_zone_id = self.parser.current_zone_id
            self.zone_label.config(text=f"Zone: {zone_name}")
            adjusted_ts = self._apply_tz(timestamp)
            self._add_feed_line(f"[{adjusted_ts.strftime('%H:%M:%S')}] Zone: {zone_name}", 'zone')
            self._refresh_zone_runs()
            
        elif event_type == 'damage':
            dmg_event = event[1]
            total_dmg = dmg_event.health_dmg + dmg_event.armor_dmg
            adjusted_ts = self._apply_tz(dmg_event.timestamp)
            line = (f"[{adjusted_ts.strftime('%H:%M:%S')}] "
                    f"{dmg_event.player_name}: {total_dmg:,} dmg → {dmg_event.npc_name}")
            self._add_feed_line(line, 'damage')
            self._refresh_current_zone()
            
        elif event_type == 'error':
            self._add_feed_line(f"Error: {event[1]}", 'error')
    
    def _add_feed_line(self, text: str, tag: str = None):
        self.feed_text.config(state='normal')
        self.feed_text.insert('end', text + '\n', tag)
        self.feed_text.see('end')
        self.feed_text.config(state='disabled')
        
        lines = int(self.feed_text.index('end-1c').split('.')[0])
        if lines > 1000:
            self.feed_text.config(state='normal')
            self.feed_text.delete('1.0', '500.0')
            self.feed_text.config(state='disabled')
    
    def _start_auto_refresh(self):
        self._session_refresh_counter = 0
        def refresh():
            if not self.loading_active:
                self._refresh_current_zone()
                self._session_refresh_counter += 1
                if self.monitoring_active and self._session_refresh_counter >= 5:
                    self._session_refresh_counter = 0
                    self._refresh_zone_runs()
                self._refresh_rolling_window()
                if hasattr(self, 'mini_window') and self.mini_window:
                    self._update_mini_window()
            self.root.after(10000, refresh)
        self.root.after(10000, refresh)
    
    def _refresh_all(self):
        self._refresh_current_zone()
        self._refresh_rolling_window()
        self._refresh_zone_runs()
        self._refresh_alias_list()
        self.tz_label.config(text=f"TZ: {self.timezone_var.get()}")
    
    def _refresh_current_zone(self):
        for item in self.zone_tree.get_children():
            self.zone_tree.delete(item)
        
        if not self.current_character:
            return
        
        zone_id = self.data_store.get_current_zone_id(self.current_character)
        if not zone_id:
            return
        
        raw_data = self.data_store.get_damage_by_zone(zone_id)
        data = group_damage_by_alias(raw_data)
        
        if not data:
            return
        
        total_dmg = sum(d['total_dmg'] for d in data)
        first_kill, last_kill, kill_count = self.data_store.get_zone_combat_times(zone_id)
        
        if first_kill and last_kill and kill_count > 0:
            combat_duration = (last_kill - first_kill).total_seconds()
            if combat_duration < 1:
                combat_duration = 1
            self.zone_info_label.config(
                text=f"Zone: {self.current_zone} | Combat: {combat_duration:.0f}s | "
                     f"Kills: {kill_count} | Total: {total_dmg:,}")
        else:
            combat_duration = 1
            self.zone_info_label.config(text=f"Zone: {self.current_zone} | No kills yet")
        
        for d in data:
            dps = d['total_dmg'] / combat_duration if combat_duration > 0 else 0
            pct = (d['total_dmg'] / total_dmg * 100) if total_dmg > 0 else 0
            self.zone_tree.insert('', 'end', values=(
                d['display_name'],
                f"{d['health_dmg']:,}",
                f"{d['armor_dmg']:,}",
                f"{d['total_dmg']:,}",
                f"{dps:.1f}",
                f"{pct:.1f}%",
                d['kills']
            ))
    
    def _refresh_rolling_window(self):
        for item in self.rolling_tree.get_children():
            self.rolling_tree.delete(item)
        
        latest_time = self.data_store.get_latest_damage_timestamp()
        if not latest_time:
            self.rolling_info_label.config(text="No damage data")
            return
        
        try:
            minutes = float(self.window_var.get())
        except ValueError:
            minutes = 5
        
        end_time = latest_time
        start_time = end_time - timedelta(minutes=minutes)
        
        raw_data = self.data_store.get_damage_in_time_range(start_time, end_time)
        data = group_damage_by_alias(raw_data)
        
        if not data:
            self.rolling_info_label.config(text=f"No damage in last {minutes:.0f} min of log")
            return
        
        total_dmg = sum(d['total_dmg'] for d in data)
        first_hits = [d['first_hit'] for d in data if d['first_hit']]
        last_hits = [d['last_hit'] for d in data if d['last_hit']]
        
        if first_hits and last_hits:
            combat_duration = (max(last_hits) - min(first_hits)).total_seconds()
            if combat_duration < 1:
                combat_duration = 1
            time_range = f"{self._format_time(min(first_hits))} - {self._format_time(max(last_hits))}"
        else:
            combat_duration = minutes * 60
            time_range = "N/A"
        
        self.rolling_info_label.config(
            text=f"Time: {time_range} | Combat: {combat_duration:.0f}s | Total: {total_dmg:,}")
        
        for d in data:
            dps = d['total_dmg'] / combat_duration if combat_duration > 0 else 0
            pct = (d['total_dmg'] / total_dmg * 100) if total_dmg > 0 else 0
            self.rolling_tree.insert('', 'end', values=(
                d['display_name'],
                f"{d['health_dmg']:,}",
                f"{d['armor_dmg']:,}",
                f"{d['total_dmg']:,}",
                f"{dps:.1f}",
                f"{pct:.1f}%",
                d['kills']
            ))
    
    def _refresh_zone_runs(self):
        saved = set(self.zones_tree.selection())
        for item in self.zones_tree.get_children():
            self.zones_tree.delete(item)

        zone_filter = self.zone_filter_var.get() if self.zone_filter_var.get() != "(All)" else None
        date_filter = self.date_filter_var.get() if self.date_filter_var.get() != "(All)" else None
        try:
            min_wis = int(self.min_wisdom_var.get() or 0)
        except ValueError:
            min_wis = 0

        instances = self.data_store.get_all_zone_instances(zone_name=zone_filter, log_date=date_filter)
        for inst in instances:
            stats = self.data_store._get_zone_stats(inst['zone_id'])
            if min_wis > stats['wisdom']:
                continue
            left = self._format_time(inst['left_time']) if inst['left_time'] else "(current)"
            self.zones_tree.insert('', 'end', iid=str(inst['zone_id']), values=(
                inst['name'],
                inst['character_name'],
                inst['log_date'] or "--",
                self._format_time(inst['entered_time']),
                left,
                f"{stats['wisdom']:,}",
                stats['kills'],
                f"{stats['total_dmg']:,}"
            ))
        for iid in saved:
            if self.zones_tree.exists(iid):
                self.zones_tree.selection_add(iid)
        self.zone_combo['values'] = ['(All)'] + self.data_store.get_unique_zone_names()
        self.date_combo['values'] = ['(All)'] + self.data_store.get_unique_log_dates()
    
    def _on_zone_runs_selection_changed(self, event):
        self._update_session_damage()
    
    def _update_session_damage(self):
        selection = self.zones_tree.selection()
        
        for item in self.session_tree.get_children():
            self.session_tree.delete(item)
        
        if not selection:
            self.selected_zones_label.config(text="No zones selected")
            return
        
        zone_ids = [int(s) for s in selection]
        
        raw_data = self.data_store.get_damage_by_zones(zone_ids)
        data = group_damage_by_alias(raw_data)
        
        if not data:
            self.selected_zones_label.config(text=f"{len(zone_ids)} zone(s) selected - No damage data")
            return
        
        first_kill, last_kill, kill_count = self.data_store.get_zones_combat_times(zone_ids)
        
        if first_kill and last_kill:
            combat_duration = (last_kill - first_kill).total_seconds()
            if combat_duration < 1:
                combat_duration = 1
        else:
            combat_duration = 1
        
        total_dmg = sum(d['total_dmg'] for d in data)
        
        self.selected_zones_label.config(
            text=f"{len(zone_ids)} zone(s) | Combat: {combat_duration:.0f}s | "
                 f"Kills: {kill_count} | Total: {total_dmg:,}")
        
        for d in data:
            dps = d['total_dmg'] / combat_duration if combat_duration > 0 else 0
            pct = (d['total_dmg'] / total_dmg * 100) if total_dmg > 0 else 0
            self.session_tree.insert('', 'end', values=(
                d['display_name'],
                f"{d['health_dmg']:,}",
                f"{d['armor_dmg']:,}",
                f"{d['total_dmg']:,}",
                f"{dps:.1f}",
                f"{pct:.1f}%",
                d['kills']
            ))
    
    def _copy_zone_runs_full(self):
        selection = self.zones_tree.selection()
        if not selection: 
            self._add_feed_line("Nothing selected", 'error'); return
        zone_ids = [int(iid) for iid in selection]
        data = group_damage_by_alias(self.data_store.get_damage_by_zones(zone_ids))
        total_dmg = sum(d['total_dmg'] for d in data)
        first, last, kills = self.data_store.get_zones_combat_times(zone_ids)
        combat_duration = (last - first).total_seconds() if first and last else 1

        lines = ["Player\tHealth\tArmor\tTotal\tDPS\t%\tKills"]
        for d in data:
            dps = d['total_dmg'] / combat_duration
            pct = d['total_dmg'] / total_dmg * 100 if total_dmg else 0
            lines.append(f"{d['display_name']}\t{d['health_dmg']:,}\t{d['armor_dmg']:,}\t{d['total_dmg']:,}\t{dps:.1f}\t{pct:.1f}%\t{d['kills']}")
        self.root.clipboard_clear()
        self.root.clipboard_append('\n'.join(lines))
    
    def _copy_zone_runs_compact(self):
        selection = self.zones_tree.selection()
        if not selection: 
            self._add_feed_line("Nothing selected", 'error'); return
        zone_ids = [int(iid) for iid in selection]
        data = group_damage_by_alias(self.data_store.get_damage_by_zones(zone_ids))
        total_dmg = sum(d['total_dmg'] for d in data)
        first, last, _ = self.data_store.get_zones_combat_times(zone_ids)
        combat_duration = (last - first).total_seconds() if first and last else 1

        lines = []
        for d in data:
            dps = d['total_dmg'] / combat_duration
            pct = d['total_dmg'] / total_dmg * 100 if total_dmg else 0
            dmg_str = format_damage_short(d['total_dmg'])
            dps_str = format_damage_short(int(dps))
            name = truncate_name(d['display_name'], 8)
            lines.append(f"{name}: {dmg_str} {dps_str}/s {pct:.0f}%")
        self.root.clipboard_clear()
        self.root.clipboard_append('\n'.join(lines))
    
    def _refresh_alias_list(self):
        for item in self.alias_tree.get_children():
            self.alias_tree.delete(item)
        
        filter_text = self.alias_filter_var.get().strip()
        players = self.data_store.get_all_players(filter_text=filter_text if filter_text else None)
        
        for player_id, original_name, alias in players:
            self.alias_tree.insert('', 'end', iid=str(player_id), values=(
                original_name, alias or "(no alias)"
            ))
    
    def _edit_alias(self, event):
        selection = self.alias_tree.selection()
        if not selection:
            return
        
        player_id = int(selection[0])
        values = self.alias_tree.item(selection[0], 'values')
        original_name = values[0]
        current_alias = values[1] if values[1] != "(no alias)" else ""
        
        new_alias = simpledialog.askstring(
            "Edit Alias",
            f"Enter alias for '{original_name}':\n(Leave blank to remove)\n\nTip: Same alias = grouped damage\nNote: Aliases are saved to file",
            initialvalue=current_alias
        )
        
        if new_alias is not None:
            self.data_store.update_player_alias(player_id, new_alias)
            self._refresh_alias_list()
            self._refresh_current_zone()
            self._refresh_rolling_window()
            self._update_session_damage()
    
    def _export_csv(self):
        selection = self.zones_tree.selection()
        if selection:
            zone_ids = [int(s) for s in selection]
            raw_data = self.data_store.get_damage_by_zones(zone_ids)
            data = group_damage_by_alias(raw_data)
            default_name = f"damage_zones_{len(zone_ids)}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        else:
            instances = self.data_store.get_all_zone_instances()
            zone_ids = [z['zone_id'] for z in instances]
            raw_data = self.data_store.get_damage_by_zones(zone_ids) if zone_ids else []
            data = group_damage_by_alias(raw_data)
            default_name = f"damage_all_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        
        filepath = filedialog.asksaveasfilename(
            title="Export Damage Data",
            defaultextension=".csv",
            filetypes=[("CSV files", "*.csv")],
            initialfile=default_name
        )
        
        if not filepath:
            return
        
        try:
            with open(filepath, 'w') as f:
                f.write("Player,Health Damage,Armor Damage,Total Damage,Kills\n")
                for d in data:
                    f.write(f"{d['display_name']},{d['health_dmg']},{d['armor_dmg']},"
                            f"{d['total_dmg']},{d['kills']}\n")
            messagebox.showinfo("Export Complete", f"Data exported to:\n{filepath}")
        except Exception as e:
            messagebox.showerror("Export Error", str(e))
    
    def _clear_all_data(self):
        if messagebox.askyesno("Clear All Data", "Clear all damage data from memory?\n(Aliases are preserved)"):
            self.data_store.clear_all_data()
            self._add_feed_line("All data cleared", 'error')
            self._refresh_all()
    
    # =========================================================================
    # MINI WINDOW
    # =========================================================================
    
    def _open_mini_window(self):
        if hasattr(self, 'mini_window') and self.mini_window:
            self.mini_window.lift()
            return
        
        self.mini_window = tk.Toplevel(self.root)
        self.mini_window.title("Damage Parser - Compact")
        self.mini_window.geometry("400x300")
        self.mini_window.attributes('-topmost', True)
        self.mini_window.protocol("WM_DELETE_WINDOW", self._close_mini_window)
        
        self.mini_view_mode = tk.StringVar(value='zone')
        
        header = ttk.Frame(self.mini_window)
        header.pack(fill='x', padx=5, pady=5)
        
        ttk.Radiobutton(header, text="Current Zone", variable=self.mini_view_mode, 
                        value='zone', command=self._update_mini_window).pack(side='left', padx=2)
        ttk.Radiobutton(header, text="Last 5 Min", variable=self.mini_view_mode,
                        value='5min', command=self._update_mini_window).pack(side='left', padx=2)
        
        self.mini_info_label = ttk.Label(self.mini_window, text="--")
        self.mini_info_label.pack(fill='x', padx=5)
        
        columns = ('Player', 'Damage', 'DPS', '%')
        self.mini_tree = ttk.Treeview(self.mini_window, columns=columns, show='headings', height=10)
        
        self.mini_tree.heading('Player', text='Player')
        self.mini_tree.heading('Damage', text='Damage')
        self.mini_tree.heading('DPS', text='DPS')
        self.mini_tree.heading('%', text='%')
        
        self.mini_tree.column('Player', width=150, anchor='w')
        self.mini_tree.column('Damage', width=80, anchor='e')
        self.mini_tree.column('DPS', width=70, anchor='e')
        self.mini_tree.column('%', width=60, anchor='e')
        
        mini_scroll = ttk.Scrollbar(self.mini_window, orient='vertical', command=self.mini_tree.yview)
        self.mini_tree.configure(yscrollcommand=mini_scroll.set)
        
        self.mini_tree.pack(side='left', fill='both', expand=True, padx=(5,0), pady=5)
        mini_scroll.pack(side='right', fill='y', padx=(0,5), pady=5)
        
        self._update_mini_window()
    
    def _close_mini_window(self):
        if hasattr(self, 'mini_window') and self.mini_window:
            self.mini_window.destroy()
            self.mini_window = None
    
    def _update_mini_window(self):
        if not hasattr(self, 'mini_window') or not self.mini_window:
            return
        
        mode = self.mini_view_mode.get()
        
        for item in self.mini_tree.get_children():
            self.mini_tree.delete(item)
        
        if mode == 'zone':
            self._update_mini_current_zone()
        else:
            self._update_mini_rolling()
    
    def _update_mini_current_zone(self):
        if not self.current_character:
            self.mini_info_label.config(text="No character detected")
            return
        
        zone_id = self.data_store.get_current_zone_id(self.current_character)
        if not zone_id:
            self.mini_info_label.config(text="No active zone")
            return
        
        raw_data = self.data_store.get_damage_by_zone(zone_id)
        data = group_damage_by_alias(raw_data)
        
        if not data:
            self.mini_info_label.config(text=f"Zone: {self.current_zone or '--'} | No damage")
            return
        
        total_dmg = sum(d['total_dmg'] for d in data)
        first_kill, last_kill, kill_count = self.data_store.get_zone_combat_times(zone_id)
        
        if first_kill and last_kill and kill_count > 0:
            combat_duration = max((last_kill - first_kill).total_seconds(), 1)
        else:
            combat_duration = 1
        
        zone_name = self.current_zone or '--'
        if len(zone_name) > 20:
            zone_name = zone_name[:17] + "..."
        self.mini_info_label.config(
            text=f"{zone_name} | {kill_count} kills | {total_dmg:,} dmg | {combat_duration:.0f}s")
        
        for d in data:
            dps = d['total_dmg'] / combat_duration if combat_duration > 0 else 0
            pct = (d['total_dmg'] / total_dmg * 100) if total_dmg > 0 else 0
            self.mini_tree.insert('', 'end', values=(
                d['display_name'],
                f"{d['total_dmg']:,}",
                f"{dps:.0f}",
                f"{pct:.1f}%"
            ))
    
    def _update_mini_rolling(self):
        latest_time = self.data_store.get_latest_damage_timestamp()
        if not latest_time:
            self.mini_info_label.config(text="No damage data")
            return
        
        minutes = 5
        end_time = latest_time
        start_time = end_time - timedelta(minutes=minutes)
        
        raw_data = self.data_store.get_damage_in_time_range(start_time, end_time)
        data = group_damage_by_alias(raw_data)
        
        if not data:
            self.mini_info_label.config(text="No damage in last 5 min")
            return
        
        total_dmg = sum(d['total_dmg'] for d in data)
        first_hits = [d['first_hit'] for d in data if d['first_hit']]
        last_hits = [d['last_hit'] for d in data if d['last_hit']]
        
        if first_hits and last_hits:
            combat_duration = max((max(last_hits) - min(first_hits)).total_seconds(), 1)
        else:
            combat_duration = minutes * 60
        
        self.mini_info_label.config(text=f"Last 5 min | {total_dmg:,} dmg | {combat_duration:.0f}s")
        
        for d in data:
            dps = d['total_dmg'] / combat_duration if combat_duration > 0 else 0
            pct = (d['total_dmg'] / total_dmg * 100) if total_dmg > 0 else 0
            self.mini_tree.insert('', 'end', values=(
                d['display_name'],
                f"{d['total_dmg']:,}",
                f"{dps:.0f}",
                f"{pct:.1f}%"
            ))
    
    def _on_close(self):
        self._stop_monitoring()
        if self.loader:
            self.loader.cancel()
        self._close_mini_window()
        self.root.destroy()
    
    def run(self):
        self.root.protocol("WM_DELETE_WINDOW", self._on_close)
        self.root.mainloop()


# ============================================================================
# MAIN
# ============================================================================

if __name__ == '__main__':
    import sys
    log_path = sys.argv[1] if len(sys.argv) > 1 else None
    app = DamageParserGUI(log_path=log_path)
    app.run()