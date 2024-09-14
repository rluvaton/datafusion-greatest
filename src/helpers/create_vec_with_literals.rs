
#[macro_export]
macro_rules! vec_with_lit {
    ( $( $x:expr ),* ) => {
        vec![$( datafusion_expr::lit($x) ),*]
    };
}

#[cfg(test)]
mod tests {
    use datafusion_expr::lit;

    #[test]
    fn test_vec_with_lit() {
        assert_eq!(vec_with_lit![1, 2, 3], vec![lit(1), lit(2), lit(3)]);
        assert_eq!(vec_with_lit![1, 2, 3], vec![lit(1), lit(2), lit(3),]);
    }
}
