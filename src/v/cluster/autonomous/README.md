# Autonomous Operations

This directory contains the implementation of Redpanda's autonomous operations system, which enables self-managing cluster operations with minimal human intervention.

## Overview

The autonomous operations system consists of several key components:

### 1. Autonomous Rebalancer (`autonomous_rebalancer.h/cc`)

Intelligent partition rebalancing system that automatically optimizes partition placement based on cluster load metrics.

**Features:**
- CPU, memory, and disk utilization monitoring
- Automatic partition rebalancing
- Predictive rebalancing based on forecasting
- Configurable thresholds and cooldown periods

### 2. Predictive Scaler (`predictive_scaler.h/cc`)

Auto-scaling system based on workload patterns and forecasting.

**Features:**
- Workload prediction
- Automatic scale-up/scale-down decisions
- Pattern-based scheduling
- Configurable scaling thresholds and cooldowns

### 3. Anomaly Detector (`anomaly_detector.h/cc`)

Multi-dimensional anomaly detection system for identifying cluster issues.

**Features:**
- Statistical anomaly detection (z-score, IQR)
- Pattern-based anomaly detection
- Correlation anomaly detection
- Configurable detection thresholds

### 4. Self-Healer (`self_healer.h/cc`)

Self-healing system that automatically remediates detected issues.

**Features:**
- Automatic remediation of common issues
- Configurable healing actions
- Cooldown periods to prevent thrashing
- Issue escalation for unresolved problems

### 5. Configuration Tuner (`configuration_tuner.h/cc`)

Self-tuning configuration optimizer that automatically adjusts cluster configuration.

**Features:**
- Performance metric collection
- A/B testing for configuration changes
- Automatic application of beneficial configurations
- Configurable improvement thresholds

### 6. Autonomous Controller (`autonomous_controller.h/cc`)

Main orchestration controller that coordinates all autonomous systems.

**Features:**
- Centralized coordination of autonomous operations
- Configurable enable/disable of individual components
- Integrated anomaly detection and healing workflow

## Configuration

Autonomous operations can be configured through cluster configuration properties:

```yaml
# Enable/disable autonomous features
autonomous_operations_enabled: true
autonomous_rebalancing_enabled: true
autonomous_scaling_enabled: false  # Disabled by default
autonomous_healing_enabled: true
autonomous_tuning_enabled: true

# Rebalancing thresholds
autonomous_rebalance_cpu_threshold: 0.8
autonomous_rebalance_memory_threshold: 0.8
autonomous_rebalance_disk_threshold: 0.85

# Scaling configuration
autonomous_scaling_min_brokers: 3
autonomous_scaling_max_brokers: 100
autonomous_scaling_scale_up_threshold: 0.8
autonomous_scaling_scale_down_threshold: 0.3
```

## Safety Considerations

The autonomous operations system includes several safety mechanisms:

1. **Cooldown Periods**: Prevent rapid successive actions
2. **Validation**: Plans are validated before execution
3. **Gradual Rollout**: Changes are applied incrementally
4. **Monitoring**: All actions are logged and monitored
5. **Escalation**: Issues that cannot be auto-resolved are escalated

## Implementation Status

This is an initial implementation providing the foundation for autonomous operations. Current implementation includes:

- ✅ Core component structure
- ✅ Basic interfaces and APIs
- ✅ Placeholder implementations
- ⏳ Full metric collection integration
- ⏳ Machine learning based prediction
- ⏳ Cloud provider integration for scaling
- ⏳ Advanced healing strategies

## Testing

Unit tests are located in `tests/autonomous_operations_test.cc`. Run tests with:

```bash
bazel test //src/v/cluster/autonomous/tests:all
```

## References

- RFC: `rfcs/20250115_autonomous_operations.md`
- Related: Partition balancer, health monitor, metrics reporter
