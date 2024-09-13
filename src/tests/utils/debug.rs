use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::util::pretty::pretty_format_batches;
use datafusion::error::Result;
use datafusion::prelude::DataFrame;

pub(crate) async fn wrap_with_debug<R, F: Fn(Vec<RecordBatch>) -> Result<R>>(data_frame: DataFrame, f: F) -> Result<R> {
    let df = data_frame.clone();
    let records = df.collect().await?;

    let cloned = records.clone();

    f(records).inspect_err(|e| {
        println!("Error: {:?}", e);
        println!("{}", pretty_format_batches(&cloned).expect("Unable to format batches"));
    })
}
