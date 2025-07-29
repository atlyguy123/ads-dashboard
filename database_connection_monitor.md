# Database Connection Monitoring Commands

# 1. Count open database files for orchestrator process:
lsof -p $(pgrep -f "python.*orchestrator") | grep -E "(\.db|\.sqlite)" | wc -l

# 2. Show specific database files currently open:
lsof -p $(pgrep -f "python.*orchestrator") | grep -E "(\.db|\.sqlite)"

# 3. Monitor total file descriptors used by orchestrator:
lsof -p $(pgrep -f "python.*orchestrator") | wc -l

# 4. Check if "Too many open files" error is resolved by refreshing dashboard multiple times

