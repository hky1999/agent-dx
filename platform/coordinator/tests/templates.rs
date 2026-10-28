mod common;
use adx_core::{
    snapshots::{Reference, Snapshot},
    templates::{Template, TemplateState},
    CheckpointArtifact, Error,
};

fn snapshot(id: &str) -> Snapshot {
    Snapshot::new(
        id.into(),
        vec![],
        serde_json::from_value(serde_json::json!({
            "id":"source", "tenant_id":"tenant", "image":"image", "runtime_class":"runsc",
            "priority":0, "resources":{"cpu_millis":100,"memory_bytes":128,"disk_bytes":0}
        }))
        .unwrap(),
        "node".into(),
        "source-1".into(),
        CheckpointArtifact {
            storage: "shared".into(),
            location: format!("artifact-{id}"),
            size_bytes: 100,
        },
    )
    .unwrap()
}

fn template(id: &str, snapshot_id: &str) -> Template {
    Template {
        id: id.into(),
        tenant_id: "tenant".into(),
        snapshot_id: snapshot_id.into(),
        name: "python".into(),
        state: TemplateState::Ready,
    }
}

#[tokio::test]
#[ignore = "requires dedicated real Redis"]
async fn template_pin_atomicity_tenant_isolation_and_restart() {
    let mut rig = common::Redis::new().await;
    let first = rig.store().await.begin(1).await.unwrap();
    let snapshot = snapshot("snapshot");
    first.publish_snapshot(snapshot.clone()).await.unwrap();
    let item = template("tpl-one", &snapshot.id);
    assert_eq!(first.import_template(item.clone()).await.unwrap(), item);
    assert_eq!(first.import_template(item.clone()).await.unwrap(), item);
    assert_eq!(first.get_template(&item.id, "tenant").await.unwrap(), item);
    assert_eq!(
        first.get_template(&item.id, "other").await,
        Err(Error::NotFound)
    );
    assert!(first.list_templates("other").await.unwrap().is_empty());
    assert!(
        matches!(first.delete_snapshot(&snapshot.id,"tenant").await,Err(Error::SnapshotInUse { template_ids, .. }) if template_ids == [item.id.clone()])
    );
    let mut wrong = item.clone();
    wrong.tenant_id = "other".into();
    wrong.id = "tpl-foreign".into();
    assert_eq!(first.import_template(wrong).await, Err(Error::NotFound));
    assert!(first.delete_template(&item.id, "other").await.is_err());
    rig.crash();
    rig.start().await;
    let second = rig.store().await.begin(1).await.unwrap();
    assert_eq!(second.get_template(&item.id, "tenant").await.unwrap(), item);
    assert!(first.delete_template(&item.id, "tenant").await.is_err());
    second
        .acquire_snapshot(
            &snapshot.id,
            "tenant",
            Reference::Restore {
                environment_id: "child".into(),
            },
        )
        .await
        .unwrap();
    second.delete_template(&item.id, "tenant").await.unwrap();
    second.delete_template(&item.id, "tenant").await.unwrap();
    assert_eq!(
        second.get_template(&item.id, "tenant").await,
        Err(Error::NotFound)
    );
    assert!(second.list_templates("tenant").await.unwrap().is_empty());
    assert_eq!(
        second.import_template(item.clone()).await,
        Err(Error::Conflict)
    );
    let saved = second.get_snapshot(&snapshot.id).await.unwrap();
    assert_eq!(
        saved.references,
        [Reference::Restore {
            environment_id: "child".into()
        }]
        .into()
    );
    assert!(second
        .delete_snapshot(&snapshot.id, "tenant")
        .await
        .is_err());
    second
        .release_snapshot(
            &snapshot.id,
            Reference::Restore {
                environment_id: "child".into(),
            },
        )
        .await
        .unwrap();
    second
        .delete_snapshot(&snapshot.id, "tenant")
        .await
        .unwrap();
}

#[tokio::test]
#[ignore = "requires dedicated real Redis"]
async fn template_identity_cannot_retarget_and_publish_races_delete() {
    let rig = common::Redis::new().await;
    let session = rig.store().await.begin(1).await.unwrap();
    session.publish_snapshot(snapshot("a")).await.unwrap();
    session.publish_snapshot(snapshot("b")).await.unwrap();
    let (a, b) = tokio::join!(
        session.import_template(template("tpl-same", "a")),
        session.import_template(template("tpl-same", "b"))
    );
    assert_ne!(a.is_ok(), b.is_ok());
    let saved = session.get_template("tpl-same", "tenant").await.unwrap();
    assert!(session
        .get_snapshot(&saved.snapshot_id)
        .await
        .unwrap()
        .references
        .contains(&saved.reference()));
    for n in 0..20 {
        let id = format!("race-{n}");
        let item = template(&format!("tpl-{n}"), &id);
        session.publish_snapshot(snapshot(&id)).await.unwrap();
        let (import, delete) = tokio::join!(
            session.import_template(item.clone()),
            session.delete_snapshot(&id, "tenant")
        );
        assert_ne!(import.is_ok(), delete.is_ok());
        if import.is_ok() {
            assert!(session
                .get_snapshot(&id)
                .await
                .unwrap()
                .references
                .contains(&item.reference()));
        } else {
            assert_eq!(
                session.get_template(&item.id, "tenant").await,
                Err(Error::NotFound)
            );
        }
    }
}
