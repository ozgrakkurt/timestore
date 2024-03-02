use std::env::temp_dir;

use anyhow::Context;
use glommio::LocalExecutor;
use timestore::IterParamsBuilder;

#[test]
fn test_empty_db() {
    let exec = LocalExecutor::default();

    exec.run(async move {
        let mut path = temp_dir();
        path.push(uuid::Uuid::new_v4().to_string());

        let (writer_factory, reader_factory) = timestore::open(
            timestore::ConfigBuilder::default()
                .path(path)
                .create_if_not_exists(true)
                .segment_length(1024)
                .tables(vec!["table0".to_owned(), "table1".to_owned()])
                .build()
                .unwrap(),
        )
        .await
        .context("open db")?;

        let mut writer = writer_factory.make().await.unwrap();
        let reader = reader_factory.make().await.unwrap();

        let res = reader.read("table0", 12).await.unwrap();
        assert!(res.is_none());
 
        let iter = reader
            .iter(IterParamsBuilder::default().from(8).to(11).build().unwrap())
            .await
            .unwrap();
        assert!(iter.is_none());

        writer
            .append(12, vec![b"123".to_vec(), b"345".to_vec()])
            .await
            .unwrap();

        let res = reader.read("table0", 12).await.unwrap().unwrap();
        assert_eq!(&*res, b"123");

        Ok::<_, anyhow::Error>(())
    })
    .unwrap();
}
