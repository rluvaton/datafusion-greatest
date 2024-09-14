use datafusion::arrow::buffer::NullBuffer;

pub(crate) trait NullBufferExt {

    /// Same as `NullBuffer::union` but if one of the buffers is `None` we return None
    ///
    /// This is useful when we want to prefer not null values
    fn union_prefer_not_null(lhs: Option<&Self>, rhs: Option<&Self>) -> Option<Self> where Self: Sized;
}

impl NullBufferExt for NullBuffer {
    fn union_prefer_not_null(lhs: Option<&Self>, rhs: Option<&Self>) -> Option<Self>
    where
        Self: Sized
    {
        match (lhs, rhs) {
            (Some(lhs), Some(rhs)) => Some(Self::new(lhs.inner() | rhs.inner())),
            (_, None) | (None, _) => None,
        }
    }
}


#[cfg(test)]
mod tests {
    use datafusion::arrow::buffer::NullBuffer;
    use super::NullBufferExt;

    #[test]
    fn union_prefer_not_null() {
        assert_eq!(NullBuffer::union_prefer_not_null(None, None), None);
        assert_eq!(NullBuffer::union_prefer_not_null(Some(&NullBuffer::new_null(2)), None), None);
        assert_eq!(NullBuffer::union_prefer_not_null(Some(&NullBuffer::new_valid(2)), None), None);
        assert_eq!(NullBuffer::union_prefer_not_null(None, Some(&NullBuffer::new_null(2))), None);
        assert_eq!(NullBuffer::union_prefer_not_null(None, Some(&NullBuffer::new_valid(2))), None);

        assert_eq!(NullBuffer::union_prefer_not_null(Some(&NullBuffer::new_null(2)), Some(&NullBuffer::new_null(2))), Some(NullBuffer::new_null(2)));
        assert_eq!(NullBuffer::union_prefer_not_null(Some(&NullBuffer::new_valid(2)), Some(&NullBuffer::new_null(2))), Some(NullBuffer::new_valid(2)));
        assert_eq!(NullBuffer::union_prefer_not_null(Some(&NullBuffer::new_null(2)), Some(&NullBuffer::new_valid(2))), Some(NullBuffer::new_valid(2)));
        assert_eq!(NullBuffer::union_prefer_not_null(Some(&NullBuffer::new_valid(2)), Some(&NullBuffer::new_valid(2))), Some(NullBuffer::new_valid(2)));
        assert_eq!(
            NullBuffer::union_prefer_not_null(
                Some(&NullBuffer::from([true, false, true, false].as_slice())),
                Some(&NullBuffer::from([true, true, false, false].as_slice()))
            ),
            Some(NullBuffer::from([true, true, true, false].as_slice()))
        );
    }

}
