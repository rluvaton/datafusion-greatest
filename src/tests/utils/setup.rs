use datafusion::dataframe::DataFrame;
use datafusion::prelude::SessionContext;
use datafusion_expr::{LogicalPlanBuilder, ScalarUDF};
use crate::GreatestUdf;


/// This creates a context with the greatest UDF registered and returns the context and the UDF
pub(crate) fn create_context() -> (SessionContext, ScalarUDF) {
    // In this example we register `GreatestUdf` as a user defined function
    // and invoke it via the DataFrame API and SQL
    // declare a new context. In Spark API, this corresponds to a new SparkSession
    let ctx = SessionContext::new();

    // create the UDF
    let greatest = ScalarUDF::from(GreatestUdf::new());

    // register the UDF with the context so it can be invoked by name and from SQL
    ctx.register_udf(greatest.clone());

    (ctx, greatest)
}

pub(crate) fn create_empty_data_frame(ctx: &SessionContext, create_one_row: bool) -> datafusion_common::Result<DataFrame> {
    let plan = LogicalPlanBuilder::empty(create_one_row).build()?;

    Ok(DataFrame::new(ctx.state(), plan))
}
