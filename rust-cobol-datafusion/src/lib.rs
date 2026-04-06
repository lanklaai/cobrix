use std::io::Read;
use std::sync::Arc;

use cobrix_rust::{
    CobolError, DecodeConfig, ParserConfig, Schema as CobolSchema, Value, parse_copybook,
    stream_rows,
};
use datafusion::arrow::array::{ArrayRef, BinaryArray, Int64Array, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::datasource::MemTable;
use datafusion::prelude::SessionContext;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum DatafusionBackendError {
    #[error("cobol parse/decode error: {0}")]
    Cobol(#[from] CobolError),
    #[error("arrow error: {0}")]
    Arrow(#[from] datafusion::arrow::error::ArrowError),
    #[error("datafusion error: {0}")]
    Datafusion(#[from] datafusion::error::DataFusionError),
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
}

pub type Result<T> = std::result::Result<T, DatafusionBackendError>;

#[derive(Debug, Clone)]
pub struct BackendConfig {
    pub parse: ParserConfig,
    pub decode: DecodeConfig,
}

impl Default for BackendConfig {
    fn default() -> Self {
        Self {
            parse: ParserConfig::default(),
            decode: DecodeConfig::default(),
        }
    }
}

pub fn build_mem_table_from_reader<R: Read>(
    copybook_text: &str,
    mut data: R,
    cfg: &BackendConfig,
) -> Result<MemTable> {
    let schema = parse_copybook(copybook_text, &cfg.parse)?;

    let mut bytes = Vec::new();
    data.read_to_end(&mut bytes)?;

    let rows = stream_rows(bytes.as_slice(), &schema, &cfg.decode)
        .collect::<std::result::Result<Vec<_>, _>>()?;

    let batch = rows_to_batch(&schema, rows)?;
    let arrow_schema = batch.schema();

    Ok(MemTable::try_new(arrow_schema, vec![vec![batch]])?)
}

pub fn register_cobol_table<R: Read>(
    ctx: &SessionContext,
    table_name: &str,
    copybook_text: &str,
    data: R,
    cfg: &BackendConfig,
) -> Result<()> {
    let table = build_mem_table_from_reader(copybook_text, data, cfg)?;
    ctx.register_table(table_name, Arc::new(table))?;
    Ok(())
}

fn rows_to_batch(schema: &CobolSchema, rows: Vec<cobrix_rust::Row>) -> Result<RecordBatch> {
    let mut fields = Vec::with_capacity(schema.fields.len());
    let mut arrays: Vec<ArrayRef> = Vec::with_capacity(schema.fields.len());

    for (idx, field) in schema.fields.iter().enumerate() {
        let mut text_values = Vec::with_capacity(rows.len());
        let mut int_values: Vec<Option<i64>> = Vec::with_capacity(rows.len());
        let mut binary_values = Vec::with_capacity(rows.len());

        let mut saw_text = false;
        let mut saw_binary = false;
        let mut saw_number = false;

        for row in &rows {
            let (_, value) = &row[idx];
            match value {
                Value::Text(v) => {
                    saw_text = true;
                    text_values.push(v.clone());
                    int_values.push(None);
                }
                Value::Number(v) => {
                    saw_number = true;
                    text_values.push(v.clone());
                    int_values.push(v.parse::<i64>().ok());
                }
                Value::Bytes(v) => {
                    saw_binary = true;
                    binary_values.push(v.clone());
                }
            }
        }

        if saw_binary {
            let f = Field::new(field.name.clone(), DataType::Binary, false);
            let a = Arc::new(BinaryArray::from_iter_values(
                binary_values.iter().map(|v| v.as_slice()),
            )) as ArrayRef;
            fields.push(f);
            arrays.push(a);
        } else if saw_number && !saw_text && int_values.iter().all(|v| v.is_some()) {
            let f = Field::new(field.name.clone(), DataType::Int64, false);
            let values: Vec<i64> = int_values.into_iter().flatten().collect();
            let a = Arc::new(Int64Array::from(values)) as ArrayRef;
            fields.push(f);
            arrays.push(a);
        } else {
            let f = Field::new(field.name.clone(), DataType::Utf8, false);
            let a = Arc::new(StringArray::from(text_values)) as ArrayRef;
            fields.push(f);
            arrays.push(a);
        }
    }

    let arrow_schema = Arc::new(Schema::new(fields));
    Ok(RecordBatch::try_new(arrow_schema, arrays)?)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn query_copybook_data_with_datafusion_sql() {
        let copybook = include_str!("../../rust-cobol/data/CUSTMAST.cbl");
        let data = include_bytes!("../../rust-cobol/data/CUSTOMER.ebcdic");

        let ctx = SessionContext::new();
        register_cobol_table(
            &ctx,
            "CUSTOMER",
            copybook,
            &data[..],
            &BackendConfig::default(),
        )
        .expect("table registered");

        let df = ctx
            .sql(
                "SELECT \"CMR-CUST-ID\", \"CMR-LAST-NAME\", \"CMR-FIRST-NAME\" \
                 FROM CUSTOMER \
                 LIMIT 1",
            )
            .await
            .expect("query");

        let batches = df.collect().await.expect("collect");
        let view = datafusion::arrow::util::pretty::pretty_format_batches(&batches)
            .expect("format")
            .to_string();

        assert!(view.contains("0000000001"));
        assert!(view.contains("JACKSON"));
        assert!(view.contains("MARGARET"));
    }
}
