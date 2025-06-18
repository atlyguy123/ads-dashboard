#!/usr/bin/env python3
"""
Analyze Valid Lifecycles - Debug Script

This script analyzes valid lifecycle patterns in the user_product_metrics table.
"""

import sqlite3
import pandas as pd
import logging
from datetime import datetime, timedelta
from collections import defaultdict, Counter
from pathlib import Path
import sys

# Add utils directory to path for database utilities
utils_path = str(Path(__file__).resolve().parent.parent.parent / "utils")
sys.path.append(utils_path)
from database_utils import get_database_path

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Configuration
DB_PATH = get_database_path('mixpanel_data')

# ... existing code ... 