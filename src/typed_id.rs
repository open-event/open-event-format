pub use type_safe_id::{StaticType, TypeSafeId};

pub trait TypedIdTestExt {
    fn new_test_id(n: u128) -> Self;
}

impl<T: StaticType> TypedIdTestExt for TypeSafeId<T> {
    fn new_test_id(n: u128) -> Self {
        Self::from_uuid(uuid::Uuid::from_u128(n))
    }
}

macro_rules! define_typed_id {
    ($(#[$doc:meta])* $name:ident, $tag:ident, $prefix:literal) => {
        #[derive(Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
        pub struct $tag;

        impl $crate::typed_id::StaticType for $tag {
            const TYPE: &'static str = $prefix;
        }

        $(#[$doc])*
        pub type $name = $crate::typed_id::TypeSafeId<$tag>;
    };
}

pub(crate) use define_typed_id;
