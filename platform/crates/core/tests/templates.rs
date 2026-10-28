use adx_core::{
    snapshots::Reference,
    templates::{Template, TemplateState},
};

#[test]
fn catalog_identity_is_tenant_owned_and_not_a_node_template() {
    let item = Template {
        id: "tpl-test".into(),
        tenant_id: "tenant".into(),
        snapshot_id: "snapshot".into(),
        name: "python".into(),
        state: TemplateState::Ready,
    };
    item.validate().unwrap();
    assert_eq!(
        item.reference(),
        Reference::CatalogTemplate {
            template_id: "tpl-test".into()
        }
    );
    let mut invalid = item.clone();
    invalid.id.clear();
    assert!(invalid.validate().is_err());
    invalid = item;
    invalid.name = "bad\nname".into();
    assert!(invalid.validate().is_err());
}

#[test]
fn template_ids_are_safe_opaque_route_components() {
    for id in ["x/y", "x?query", "x#fragment", " x", "中文"] {
        let item = Template {
            id: id.into(),
            tenant_id: "tenant".into(),
            snapshot_id: "snapshot".into(),
            name: "python".into(),
            state: TemplateState::Ready,
        };
        assert!(item.validate().is_err(), "accepted invalid route ID {id}");
    }
}
