#[derive(Clone, PartialEq, ::prost::Message)]
pub struct TestTable {
    #[prost(uint64, tag = "1")]
    pub parallelism: u64,
    #[prost(uint64, tag = "2")]
    pub global_limit: u64,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct TestTableExec {
    #[prost(message, optional, tag = "1")]
    pub table: ::core::option::Option<TestTable>,
    #[prost(uint64, optional, tag = "2")]
    pub limit: ::core::option::Option<u64>,
    #[prost(uint64, repeated, tag = "3")]
    pub projection: ::prost::alloc::vec::Vec<u64>,
    #[prost(uint64, tag = "4")]
    pub global_limit: u64,
}
