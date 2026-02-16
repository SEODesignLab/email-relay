#!/bin/bash
# POP Audit Batch Runner - Run audits one at a time

API_BASE="https://email-relay-xjqx.onrender.com"
KEY="sdl-prospector-2026"
LOG_FILE="/Users/xzin/Documents/Cline/Websites/email-relay/pop_audit_results.log"

# Priority hot prospects
PRIORITY_HOT=(249 280 299 622 774 775)

# Other hot prospects (from the list)
OTHER_HOT=(602 367 334 328 325 349 281 331 330 327)

# Warm prospects (first 26)
WARM=(359 364 341 307 326 1 225 229 230 238 239 240 243 255 256 276 278 279 289 290 291 293 335 336 710 723)

echo "=== POP Audit Batch Run - $(date) ===" | tee -a "$LOG_FILE"
echo "" | tee -a "$LOG_FILE"

# Function to run a single audit
run_audit() {
    local id=$1
    local name=$2
    local type=$3
    
    echo "[$(date '+%H:%M:%S')] Starting $type audit for ID $id ($name)..." | tee -a "$LOG_FILE"
    
    # Start the audit
    response=$(curl -s -X POST "$API_BASE/api/pop_audit_start?key=$KEY" \
        -H "Content-Type: application/json" \
        -d "{\"prospect_id\": $id}")
    
    job_id=$(echo "$response" | python3 -c "import sys, json; d=json.load(sys.stdin); print(d.get('job_id', ''))")
    
    if [ -z "$job_id" ]; then
        echo "[$(date '+%H:%M:%S')] ERROR: Failed to start audit for ID $id" | tee -a "$LOG_FILE"
        echo "Response: $response" | tee -a "$LOG_FILE"
        return 1
    fi
    
    echo "[$(date '+%H:%M:%S')] Job started: $job_id" | tee -a "$LOG_FILE"
    
    # Poll for completion (up to 30 minutes)
    for i in {1..120}; do
        sleep 15
        status_resp=$(curl -s "$API_BASE/api/pop_audit_status?key=$KEY&job_id=$job_id")
        status=$(echo "$status_resp" | python3 -c "import sys, json; d=json.load(sys.stdin); print(d.get('status', 'unknown'))")
        
        if [ "$status" = "complete" ]; then
            echo "[$(date '+%H:%M:%S')] ✓ SUCCESS: ID $id completed" | tee -a "$LOG_FILE"
            echo "$status_resp" | python3 -c "import sys, json; d=json.load(sys.stdin); print(json.dumps(d.get('result', {}), indent=2))" | head -20 | tee -a "$LOG_FILE"
            return 0
        elif [ "$status" = "error" ]; then
            echo "[$(date '+%H:%M:%S')] ✗ FAILED: ID $id" | tee -a "$LOG_FILE"
            echo "$status_resp" | python3 -c "import sys, json; d=json.load(sys.stdin); print(d.get('error', 'Unknown error'))" | tee -a "$LOG_FILE"
            return 1
        fi
        
        # Show progress every 2 minutes
        if [ $((i % 8)) -eq 0 ]; then
            progress=$(echo "$status_resp" | python3 -c "import sys, json; d=json.load(sys.stdin); print(d.get('progress', 'Processing...'))")
            echo "[$(date '+%H:%M:%S')] Still running... $progress" | tee -a "$LOG_FILE"
        fi
    done
    
    echo "[$(date '+%H:%M:%S')] ✗ TIMEOUT: ID $id after 30 minutes" | tee -a "$LOG_FILE"
    return 1
}

# Run priority hot prospects
echo "=== PHASE 1: Priority Hot Prospects ===" | tee -a "$LOG_FILE"
for id in "${PRIORITY_HOT[@]}"; do
    run_audit "$id" "Priority-$id" "hot"
    echo "---" | tee -a "$LOG_FILE"
done

# Run other hot prospects  
echo "" | tee -a "$LOG_FILE"
echo "=== PHASE 2: Other Hot Prospects ===" | tee -a "$LOG_FILE"
for id in "${OTHER_HOT[@]}"; do
    run_audit "$id" "Hot-$id" "hot"
    echo "---" | tee -a "$LOG_FILE"
done

# Run warm prospects
echo "" | tee -a "$LOG_FILE"
echo "=== PHASE 3: Warm Prospects ===" | tee -a "$LOG_FILE"
for id in "${WARM[@]}"; do
    run_audit "$id" "Warm-$id" "warm"
    echo "---" | tee -a "$LOG_FILE"
done

echo "" | tee -a "$LOG_FILE"
echo "=== Batch Run Complete - $(date) ===" | tee -a "$LOG_FILE"
