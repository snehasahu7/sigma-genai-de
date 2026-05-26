from typing import Dict, List, Tuple, Union
import pyspark.sql.functions as F
from pyspark.sql import DataFrame

def detect_schema_drift(expected_schema: Dict[str, str], actual_schema: Dict[str, str]) -> Dict[str, Union[Dict[str, str], str, bool]]:
    new_columns = {k: v for k, v in actual_schema.items() if k not in expected_schema}
    removed_columns = {k: v for k, v in expected_schema.items() if k not in actual_schema}
    type_changes = {k: (expected_schema[k], actual_schema[k]) for k in expected_schema if expected_schema[k]!= actual_schema[k]}
    has_drift = bool(new_columns) or bool(removed_columns) or bool(type_changes)
    
    drift_severity = 'NONE'
    if removed_columns:
        drift_severity = 'BREAKING'
    elif any(not v.endswith('null') for v in new_columns.values()):
        drift_severity = 'HIGH'
    elif any(v.endswith('null') for v in new_columns.values()):
        drift_severity = 'LOW'

    return {
        "new_columns": new_columns,
        "removed_columns": removed_columns,
        "type_changes": type_changes,
        "has_drift": has_drift,
        "drift_severity": drift_severity
    }

def decide_action(drift_report: Dict[str, Union[Dict[str, str], str, bool]]) -> Dict[str, Dict[str, Union[str, str, str]]]:
    decisions = {}
    for column, dtype in drift_report['new_columns'].items():
        if dtype.endswith('null') and dtype.startswith('string'):
            decisions[column] = {'action': 'ADD_TO_SCHEMA','reason': 'New nullable string column', 'risk_level': 'LOW'}
        elif dtype.endswith('null') and dtype.startswith('float'):
            decisions[column] = {'action': 'FLAG_ANOMALY','reason': 'New nullable float column', 'risk_level': 'HIGH'}
        elif not dtype.endswith('null') and dtype.startswith('string'):
            decisions[column] = {'action': 'ADD_TO_SCHEMA','reason': 'New non-nullable string column', 'risk_level': 'HIGH'}
        elif not dtype.endswith('null') and dtype.startswith('float'):
            decisions[column] = {'action': 'FLAG_ANOMALY','reason': 'New non-nullable float column', 'risk_level': 'HIGH'}
    for column, (old_type, new_type) in drift_report['type_changes'].items():
        if old_type!= new_type and new_type.endswith('null'):
            decisions[column] = {'action': 'ADD_TO_SCHEMA','reason': f'Type widened from {old_type} to {new_type}', 'risk_level': 'LOW'}
        elif old_type!= new_type and old_type.endswith('null'):
            decisions[column] = {'action': 'FLAG_ANOMALY','reason': f'Type narrowed from {old_type} to {new_type}', 'risk_level': 'HIGH'}
    for column in drift_report['removed_columns']:
        decisions[column] = {'action': 'HALT','reason': 'Column removed', 'risk_level': 'BREAKING'}
    return decisions

def apply_schema_evolution(spark_df: DataFrame, decisions: Dict[str, Dict[str, Union[str, str, str]]], updated_schema: Dict[str, str]) -> Tuple[DataFrame, List[str]]:
    migration_notes = []
    for column, decision in decisions.items():
        if decision['action'] == 'DROP_SILENTLY':
            spark_df = spark_df.drop(column)
        elif decision['action'] == 'ADD_TO_SCHEMA':
            migration_notes.append(f"Added new column: {column} with type {updated_schema[column]}")
        elif decision['action'] == 'FLAG_ANOMALY':
            spark_df = spark_df.withColumn(f"{column}_anomaly", F.when(F.col(column).isNull(), True).otherwise(False))
            migration_notes.append(f"Flagged anomalies in column: {column}")
        elif decision['action'] == 'HALT':
            raise ValueError(f"Cannot silently drop column: {column}. This will break downstream queries.")
    return spark_df, migration_notes

def handle_drift(expected_schema: Dict[str, str], actual_schema: Dict[str, str], spark_df: DataFrame = None) -> Dict[str, Union[Dict, List, Dict[str, Dict]]]:
    drift_report = detect_schema_drift(expected_schema, actual_schema)
    if not drift_report['has_drift']:
        print("No schema drift detected.")
        return drift_report
    
    decisions = decide_action(drift_report)
    if spark_df is not None:
        updated_schema = {k: v for k, v in actual_schema.items()}
        spark_df, migration_notes = apply_schema_evolution(spark_df, decisions, updated_schema)
        drift_report['migration_notes'] = migration_notes
    
    print("Schema drift detected. Here are the details:")
    print(f"New columns: {drift_report['new_columns']}")
    print(f"Removed columns: {drift_report['removed_columns']}")
    print(f"Type changes: {drift_report['type_changes']}")
    print(f"Drift severity: {drift_report['drift_severity']}")
    print(f"Actions decided: {decisions}")
    
    return drift_report
