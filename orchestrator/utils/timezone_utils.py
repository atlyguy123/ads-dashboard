#!/usr/bin/env python3
"""
Timezone utilities for consistent time handling across the system.
Provides centralized timezone conversion and formatting functions.
"""

import datetime
import pytz
from typing import Optional, Union

# Handle imports for both orchestrator context and external script context
try:
    from ..config import config
except ImportError:
    # Fallback for when imported from external scripts
    try:
        from orchestrator.config import config
    except ImportError:
        import sys
        from pathlib import Path
        # Add orchestrator directory to path and try again
        orchestrator_dir = Path(__file__).parent.parent
        if str(orchestrator_dir) not in sys.path:
            sys.path.insert(0, str(orchestrator_dir))
        from config import config

def get_system_timezone() -> pytz.BaseTzInfo:
    """Get the configured system timezone."""
    return pytz.timezone(config.DEFAULT_TIMEZONE)

def get_display_timezone() -> pytz.BaseTzInfo:
    """Get the configured display timezone."""
    return pytz.timezone(config.DISPLAY_TIMEZONE)

def now_in_timezone(timezone: Optional[str] = None) -> datetime.datetime:
    """Get current time in specified timezone."""
    tz = pytz.timezone(timezone) if timezone else get_system_timezone()
    return datetime.datetime.now(tz)

def utc_to_local(dt: datetime.datetime, timezone: Optional[str] = None) -> datetime.datetime:
    """Convert UTC datetime to local timezone."""
    if dt.tzinfo is None:
        dt = pytz.utc.localize(dt)
    elif dt.tzinfo != pytz.utc:
        dt = dt.astimezone(pytz.utc)
    
    target_tz = pytz.timezone(timezone) if timezone else get_system_timezone()
    return dt.astimezone(target_tz)

def local_to_utc(dt: datetime.datetime, source_timezone: Optional[str] = None) -> datetime.datetime:
    """Convert local datetime to UTC."""
    if dt.tzinfo is None:
        source_tz = pytz.timezone(source_timezone) if source_timezone else get_system_timezone()
        dt = source_tz.localize(dt)
    
    return dt.astimezone(pytz.utc)

def format_for_display(dt: datetime.datetime, timezone: Optional[str] = None) -> str:
    """Format datetime for display in configured timezone."""
    if dt.tzinfo is None:
        dt = pytz.utc.localize(dt)
    
    display_tz = pytz.timezone(timezone) if timezone else get_display_timezone()
    local_dt = dt.astimezone(display_tz)
    return local_dt.strftime('%Y-%m-%d %H:%M:%S %Z')

def parse_date_string(date_str: str, timezone: Optional[str] = None) -> datetime.datetime:
    """Parse date string and localize to specified timezone."""
    # Handle ISO format with Z suffix
    if date_str.endswith('Z'):
        date_str = date_str[:-1] + '+00:00'
    
    dt = datetime.datetime.fromisoformat(date_str)
    
    if dt.tzinfo is None:
        source_tz = pytz.timezone(timezone) if timezone else get_system_timezone()
        dt = source_tz.localize(dt)
    
    return dt

def get_timezone_list():
    """Get list of commonly used timezones with descriptions."""
    return {
        'America/New_York': 'Eastern Time (ET/EDT)',
        'America/Chicago': 'Central Time (CT/CDT)',
        'America/Denver': 'Mountain Time (MT/MDT)',
        'America/Los_Angeles': 'Pacific Time (PT/PDT)',
        'America/Phoenix': 'Arizona Time (no DST)',
        'America/Anchorage': 'Alaska Time (AKST/AKDT)',
        'Pacific/Honolulu': 'Hawaii Time (HST)',
        'Asia/Jerusalem': 'Israel Time (IST/IDT)',
        'Europe/London': 'GMT/BST',
        'Europe/Paris': 'CET/CEST',
        'Europe/Berlin': 'CET/CEST',
        'Asia/Tokyo': 'Japan Time (JST)',
        'Asia/Shanghai': 'China Time (CST)',
        'Australia/Sydney': 'AEST/AEDT',
        'UTC': 'UTC (no DST)'
    } 