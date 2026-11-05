"""Four independently retryable stages for the supplied Olist CPU baseline."""
from __future__ import annotations

import csv
import hashlib
import math
import os
from datetime import datetime
from pathlib import Path

NUMERIC_COLUMNS = ('price', 'freight', 'weight', 'n_items', 'installments', 'est_days')
CATEGORICAL_COLUMNS = ('customer_state', 'seller_state', 'category', 'payment_type')
REQUIRED_COLUMNS = ('order_id', 'order_purchase_timestamp', *NUMERIC_COLUMNS,
                    *CATEGORICAL_COLUMNS, 'late')


def _workdir() -> Path:
    return Path(os.environ.get('OLIST_WORKDIR', '/opt/olist/work')).resolve()


def _sha256(path: Path) -> str:
    with path.open('rb') as stream:
        return hashlib.file_digest(stream, 'sha256').hexdigest()


def extract(run_key: str) -> str:
    """Copy a consistent, ordered PostgreSQL snapshot to an atomic UTF-8 CSV.

    A retry replaces the same file. Different DAG runs have different directories;
    hashing the run key also prevents path traversal through the Airflow run ID.
    """
    import psycopg

    if not isinstance(run_key, str) or not run_key:
        raise ValueError('run_key must be a nonempty string')
    directory = _workdir() / 'runs' / hashlib.sha256(run_key.encode('utf-8')).hexdigest()
    directory.mkdir(parents=True, exist_ok=True)
    snapshot = directory / 'snapshot.csv'
    temporary = directory / 'snapshot.csv.tmp'
    dsn = os.environ['DATABASE_URL']
    if dsn.startswith('postgresql+psycopg://'):
        dsn = 'postgresql://' + dsn[len('postgresql+psycopg://'):]
    query = ('COPY (SELECT ' + ', '.join(REQUIRED_COLUMNS) +
             ' FROM public.orders ORDER BY order_purchase_timestamp, order_id) '
             "TO STDOUT WITH (FORMAT CSV, HEADER TRUE, ENCODING 'UTF8')")
    try:
        with psycopg.connect(dsn, connect_timeout=10, options='-c statement_timeout=120000') as connection:
            with connection.cursor() as cursor, temporary.open('wb') as stream:
                with cursor.copy(query) as copy:
                    for chunk in copy:
                        stream.write(chunk)
        temporary.replace(snapshot)
    finally:
        temporary.unlink(missing_ok=True)
    return str(snapshot)


def validate(snapshot: str) -> str:
    """Read only: validate a local, plain UTF-8 CSV and return its absolute path.

    No database/model imports, writes, directory creation, processes or network.
    Categorical blanks are allowed: the supplied model handles missing categories.
    """
    path = Path(snapshot).resolve()
    if path.suffix != '.csv' or not path.is_file():
        raise ValueError('snapshot must be an existing plain .csv file')
    with path.open('r', encoding='utf-8', newline='') as stream:
        reader = csv.reader(stream, strict=True)
        header = next(reader, None)
        if not header:
            raise ValueError('snapshot is empty')
        if len(set(header)) != len(header):
            raise ValueError('CSV column names must be unique')
        missing = set(REQUIRED_COLUMNS) - set(header)
        if missing:
            raise ValueError(f'Missing required columns: {sorted(missing)}')
        seen = set()
        for line, values in enumerate(reader, start=2):
            if len(values) != len(header):
                raise ValueError(f'Invalid CSV field count at line {line}')
            row = dict(zip(header, values))
            order_id = row['order_id'].strip()
            if not order_id or order_id in seen:
                raise ValueError(f'Missing or duplicate order_id at line {line}')
            seen.add(order_id)
            try:
                datetime.fromisoformat(row['order_purchase_timestamp'])
            except ValueError as exc:
                raise ValueError(f'Invalid purchase timestamp at line {line}') from exc
            for column in (*NUMERIC_COLUMNS, 'late'):
                try:
                    value = float(row[column])
                except ValueError as exc:
                    raise ValueError(f'{column} must be numeric at line {line}') from exc
                if not math.isfinite(value):
                    raise ValueError(f'{column} must be finite at line {line}')
                if column == 'late' and value not in (0, 1):
                    raise ValueError(f'late must be 0 or 1 at line {line}')
        if not seen:
            raise ValueError('snapshot contains no orders')
    return str(path)


def _read_frame(source):
    import pandas as pd

    return pd.read_csv(source, encoding='utf-8', float_precision='round_trip',
                       dtype={name: str for name in ('order_id', *CATEGORICAL_COLUMNS)})


def prepare(snapshot: str) -> dict:
    """Use the notebook's chronological split; retain the original CSV digest."""
    import io
    from olist_model import split_by_time

    path = Path(snapshot).resolve()
    # Hash and parse the exact same bytes, before any serialization/transformation.
    raw = path.read_bytes()
    snapshot_sha256 = hashlib.sha256(raw).hexdigest()
    cutoff = os.environ.get('OLIST_CUTOFF', '2017-07-31')
    train_df, validation_df = split_by_time(_read_frame(io.BytesIO(raw)), cutoff=cutoff)
    key = hashlib.sha256(f'{path}\n{snapshot_sha256}\n{cutoff}'.encode()).hexdigest()
    directory = _workdir() / 'prepared' / key
    directory.mkdir(parents=True, exist_ok=True)
    paths = {}
    for name, frame in (('train', train_df), ('validation', validation_df)):
        output = directory / f'{name}.csv'
        temporary = output.with_suffix('.csv.tmp')
        try:
            frame.to_csv(temporary, index=False, encoding='utf-8')
            temporary.replace(output)
        finally:
            temporary.unlink(missing_ok=True)
        paths[f'{name}_path'] = str(output)
        paths[f'{name}_sha256'] = _sha256(output)
    return {**paths, 'snapshot_path': str(path), 'snapshot_sha256': snapshot_sha256,
            'cutoff': cutoff, 'train_rows': len(train_df), 'validation_rows': len(validation_df)}


def train(prepared: dict, *, dag_run_id: str, epochs=None, seed=None) -> str:
    """Fit the supplied train-only transforms/model and log a complete MLflow run."""
    import mlflow
    from olist_model import fit_model, save_predictor

    if not isinstance(dag_run_id, str) or not dag_run_id:
        raise ValueError('dag_run_id must be the actual Airflow run_id')
    epochs = int(os.environ.get('OLIST_EPOCHS', '5')) if epochs is None else epochs
    seed = int(os.environ.get('OLIST_SEED', '42')) if seed is None else seed
    lr = float(os.environ.get('OLIST_LR', '0.003'))
    for name in ('snapshot', 'train', 'validation'):
        if _sha256(Path(prepared[f'{name}_path'])) != prepared[f'{name}_sha256']:
            raise ValueError(f'{name} CSV changed after prepare')
    train_df = _read_frame(prepared['train_path'])
    validation_df = _read_frame(prepared['validation_path'])
    if len(train_df) != prepared['train_rows'] or len(validation_df) != prepared['validation_rows']:
        raise ValueError('Prepared row counts do not match the metadata')
    mlflow.set_tracking_uri(os.environ['MLFLOW_TRACKING_URI'])
    mlflow.set_experiment(os.environ.get('MLFLOW_EXPERIMENT_NAME', 'olist_training'))
    with mlflow.start_run(run_name=dag_run_id, tags={'dag_run_id': dag_run_id}) as run:
        mlflow.log_params({'epochs': epochs, 'seed': seed, 'lr': lr,
                           **{key: prepared[key] for key in (
                               'cutoff', 'train_rows', 'validation_rows', 'snapshot_sha256')}})
        predictor, history = fit_model(train_df, validation_df, epochs=epochs, seed=seed, lr=lr)
        for row in history:
            mlflow.log_metrics({'train_loss': row['train_loss'],
                                'validation_pr_auc': row['validation_pr_auc']}, step=row['epoch'])
        directory = _workdir() / 'models' / run.info.run_id
        save_predictor(predictor, directory)
        mlflow.log_artifacts(str(directory), artifact_path='model')
        return run.info.run_id
