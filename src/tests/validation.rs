#[cfg(test)]
mod validation_tests {
    use crate::tests::utils::{create_context, create_empty_data_frame};
    use crate::vec_with_lit;
    use datafusion::dataframe::DataFrame;
    use datafusion::error::Result;
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


        // TODO - add more types, there are a lot more
        // the first element is the type that is incompatible with the rest
        let cases = vec![
            vec_with_lit![1i8, 1u8, 1u16, 1u32, 1u64, true, "hello".as_bytes()],
            vec_with_lit![1i16, 1u16, 1u32, 1u64, true, "hello".as_bytes()],
            vec_with_lit![1i32, 1u32, 1u64, true, "hello".as_bytes()],
            vec_with_lit![1i64, 1u64, true, "hello".as_bytes()],

            vec_with_lit![1u8, true, "hello".as_bytes()],
            vec_with_lit![1u16, true, "hello".as_bytes()],
            vec_with_lit![1u32, true, "hello".as_bytes()],
            vec_with_lit![1u64, true, "hello".as_bytes()],
            vec_with_lit![1f32, true, "hello".as_bytes()],
            vec_with_lit![1f64, true, "hello".as_bytes()],

            vec_with_lit![true, "hello".as_bytes()],
        ];

        for one_case in cases {
            let (type_to_test, rest) = one_case.split_at(1);
            let type_to_test = type_to_test.first().unwrap();

            for incompatible_with in rest {
                // Making sure the order of the arguments does not matter
                let error = call_greatest_with_args(vec![type_to_test.clone(), incompatible_with.clone()]).expect_err(format!("Expected an error for types {:?} and {:?}", type_to_test, incompatible_with).as_str());
                assert!(error.message().contains("Cannot find a common type for arguments"), "Error message: {}", error.message());

                let error = call_greatest_with_args(vec![incompatible_with.clone(), type_to_test.clone()]).expect_err(format!("Expected an error for types {:?} and {:?}", incompatible_with, type_to_test).as_str());
                assert!(error.message().contains("Cannot find a common type for arguments"), "Error message: {}", error.message());
            }
        }
    }
}
