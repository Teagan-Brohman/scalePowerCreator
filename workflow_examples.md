# Complete Workflow Examples

## Basic Usage

### Full Workflow (Generate → Execute → Process → Cleanup)
```bash
# Basic run with default settings
python3 tools/complete_workflow.py \
    --flux-json sample_flux_data.json \
    --power-time origen_cards.txt

# Custom run with more workers
python3 tools/complete_workflow.py \
    --flux-json sample_flux_data.json \
    --power-time origen_cards.txt \
    --scale-workers 16 \
    --parse-workers 24 \
    --run-name "high_burnup_analysis"

# With moderate cleanup (archives large files)
python3 tools/complete_workflow.py \
    --flux-json sample_flux_data.json \
    --power-time origen_cards.txt \
    --cleanup moderate
```

## Resume Workflow

### Resume from SCALE Execution (if generation already done)
```bash
python3 tools/complete_workflow.py \
    --flux-json sample_flux_data.json \
    --power-time origen_cards.txt \
    --resume-from scale-execution \
    --run-dir scale_runs/workflow_2025-09-08_19-03-30
```

### Resume from MCNP Generation (if SCALE jobs completed)
```bash
python3 tools/complete_workflow.py \
    --flux-json sample_flux_data.json \
    --power-time origen_cards.txt \
    --resume-from mcnp-generation \
    --run-dir scale_runs/workflow_2025-09-08_19-03-30
```

## Expected Output Structure

```
scale_runs/workflow_2025-09-08_19-03-30/
├── inputs/                          # SCALE input files
│   ├── element_Assembly_*.inp       # Generated SCALE inputs
│   ├── element_Assembly_*.out       # SCALE outputs
│   ├── element_Assembly_*.msg       # SCALE message files
│   └── element_mapping.json        # Element to assembly mapping
├── mcnp_cards/                      # MCNP material cards
│   ├── mcnp_materials_all.txt      # Combined material cards
│   ├── processing_summary.json     # Processing statistics
│   └── materials_database.db       # SQLite database
├── archive/                         # Archived files (if cleanup > minimal)
│   └── scale_outputs.zip          # Compressed SCALE outputs
├── logs/                           # Log files
├── workflow_summary.json          # Complete workflow summary
└── workflow_workflow_*.log        # Detailed execution log
```

## Output Files

### MCNP Material Cards (`mcnp_materials_all.txt`)
```
c MCNP Material Cards from Parallel ORIGEN Processing
c Generated from: 252 element files
c Processing time: 2025-09-08 19:46:35
c
c Element: element_Assembly_MTR-F-001_E001
M200 nlib=00c
     2004 -2.158114e-10
     13027 -3.222553e-01
     14028 -4.545364e-02
     92235 -1.240806e-01
     92238 -5.041888e-01
c
c Element: element_Assembly_MTR-F-001_E002
M201 nlib=00c
     ...
```

### Processing Summary (`processing_summary.json`)
```json
{
  "processing_info": {
    "total_elements": 252,
    "successful_elements": 252,
    "failed_elements": 0,
    "max_workers": 16,
    "executor_type": "thread",
    "processing_date": "2025-09-08T19:46:35"
  },
  "elements": {
    "element_Assembly_MTR-F-001_E001": {
      "status": "completed",
      "material_id": 200,
      "total_mass_g": 100.74,
      "density_g_cm3": 5.52
    }
  }
}
```

## Performance Examples

### Small Test Run (18 elements)
```bash
# Test with single assembly
python3 tools/complete_workflow.py \
    --flux-json sample_flux_data.json \
    --power-time origen_cards.txt \
    --run-name "test_single_assembly" \
    --scale-workers 4 \
    --parse-workers 8
```

### Full Production Run (252 elements)
```bash
# Production run with maximum parallelism
python3 tools/complete_workflow.py \
    --flux-json sample_flux_data.json \
    --power-time origen_cards.txt \
    --run-name "production_full_core" \
    --scale-workers 16 \
    --parse-workers 24 \
    --cleanup moderate \
    --verbose
```
tools/complete_workflow.py --flux-json sample_flux_data.json --power-time origen_cards.txt  --run-name "production_full_core"  --scale-workers 16 --parse-workers 24 --cleanup moderate --verbose
    
    --power-time origen_cards.txt 
    --run-name "production_full_core"CD  
    --scale-workers 16 
    --parse-workers 24 
    --cleanup moderate 
    --verbose
```

## Typical Execution Times

| Step | Small Run (18 elements) | Full Run (252 elements) |
|------|------------------------|--------------------------|
| SCALE Generation | 30 seconds | 2 minutes |
| SCALE Execution | 5-10 minutes | 30-60 minutes |
| MCNP Generation | 1 minute | 3-5 minutes |
| **Total** | **6-11 minutes** | **35-67 minutes** |

## Troubleshooting

### Resume if SCALE jobs fail
```bash
# Fix any issues, then resume from SCALE execution
python3 tools/complete_workflow.py \
    --resume-from scale-execution \
    --run-dir scale_runs/your_failed_run
```

### Only generate MCNP cards from existing outputs
```bash
# Skip to MCNP generation if you have SCALE outputs
python3 tools/complete_workflow.py \
    --resume-from mcnp-generation \
    --run-dir scale_runs/existing_scale_outputs
```

### Check workflow status
```bash
# View the workflow summary
cat scale_runs/workflow_*/workflow_summary.json

# Check the processing log
tail -f workflow_*.log
```