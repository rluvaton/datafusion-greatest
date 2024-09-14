#[cfg(test)]
mod validation_tests {
    use datafusion::error::Result;
    use datafusion::dataframe::DataFrame;
    use crate::tests::utils::{create_context, create_empty_data_frame};
    use datafusion_expr::{lit, Expr};

    fn call_greatest_with_args(args: Vec<Expr>) -> Result<DataFrame> {
        let (ctx, greatest) = create_context();

        let df = create_empty_data_frame(&ctx, true)?;

        df.select(vec![greatest.call(args)])
    }

    #[tokio::test]
    async fn test_no_arguments() {
        let error = call_greatest_with_args(vec![]).unwrap_err();

        assert!(error.message().contains("greatest does not support zero arguments"), "Error message: {}", error.message());
    }

    #[tokio::test]
    async fn test_one_argument() {
        let error = call_greatest_with_args(vec![lit(1)]).unwrap_err();

        assert!(error.message().contains("greatest was called with 1 arguments. It requires at least 2"), "Error message: {}", error.message());

    }

    #[tokio::test]
    async fn test_incompatible_types() {
        let error = call_greatest_with_args(vec![lit(1i32), lit(true)]).unwrap_err();
        assert!(error.message().contains("Cannot find a common type for arguments, incompatible Int32 and Boolean"), "Error message: {}", error.message());

        let error = call_greatest_with_args(vec![lit(1i32), lit("hello".as_bytes())]).unwrap_err();
        assert!(error.message().contains("Cannot find a common type for arguments, incompatible Int32 and Binary"), "Error message: {}", error.message());

        let error = call_greatest_with_args(vec![lit(1i32), lit(vec![])]).unwrap_err();
        assert!(error.message().contains("Cannot find a common type for arguments, incompatible Int32 and Binary"), "Error message: {}", error.message());

        // TODO - add more tests
    }
}
