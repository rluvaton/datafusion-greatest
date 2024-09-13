#[cfg(test)]
mod scalars_tests {
    /// These tests check that calling greatest only on scalars values
    ///
    /// This does not include columns
    ///


    use crate::tests::utils::{create_context, create_empty_data_frame, get_result_as_matrix};
    use datafusion::arrow::datatypes::Int8Type;
    use datafusion_expr::lit;


    #[tokio::test]
    async fn i8_without_nulls() {
        let (ctx, greatest) = create_context();

        let df = create_empty_data_frame(&ctx, true).unwrap();

        let df = df.select(vec![greatest.call(vec![lit(2i8), lit(10i8)])]).unwrap();

        let results = get_result_as_matrix::<Int8Type>(df).await.unwrap();

        assert_eq!(results, vec![
            vec![Some(10)]
        ]);
    }
}
