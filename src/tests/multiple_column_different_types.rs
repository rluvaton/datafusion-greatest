#[cfg(test)]
mod multiple_column_different_types_tests {

    /// These tests check that calling greatest on multiple columns with different types
    /// This mean that we check coercion of types and the value
    ///
    /// TODO - we should also check not losing precision or data when comparing types
    ///

    #[tokio::test]
    async fn test_invalid_expr() {
        todo!();
    }
}
