mod support;

#[tokio::test]
async fn shared_transport_fixture_satisfies_the_postgres_backed_lifecycle_contract() {
    support::lifecycle_conformance(&support::TestStore::default())
        .await
        .unwrap();
}
